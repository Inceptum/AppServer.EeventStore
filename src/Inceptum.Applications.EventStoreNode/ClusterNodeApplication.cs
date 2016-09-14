using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Castle.Core.Logging;
using EventStore.Common.Exceptions;
using EventStore.Common.Options;
using EventStore.Common.Utils;
using EventStore.Core;
using EventStore.Core.Authentication;
using EventStore.Core.Cluster.Settings;
using EventStore.Core.Services.Gossip;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Services.Transport.Http.Controllers;
using EventStore.Core.Settings;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.Util;
using EventStore.Projections.Core;
using Inceptum.AppServer;
using EventStore.Common.Log;
using EventStore.Core.Data;
using EventStore.Core.PluginModel;
using EventStore.Core.Services.PersistentSubscription.ConsumerStrategy;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using NLog.Fluent;


namespace Inceptum.Applications.EventStoreNode.Cluster
{
    public class ClusterNodeApplication : IHostedApplication, IDisposable
    {
        private readonly ILoggerFactory m_LoggerFactory;
        private readonly Castle.Core.Logging.ILogger m_Logger;
        private readonly ClusterNodeConfiguration m_Configuration;
        
        private ExclusiveDbLock m_DbLock;
        private ProjectionsSubsystem m_Projections;
        private ClusterVNode m_Node;
        private ClusterNodeMutex m_ClusterNodeMutex;
        
        public ClusterNodeApplication(ILoggerFactory loggerFactory, Castle.Core.Logging.ILogger logger, ClusterNodeConfiguration configuration)
        {
            if (loggerFactory == null) throw new ArgumentNullException("loggerFactory");
            if (logger == null) throw new ArgumentNullException("logger");
            if (configuration == null) throw new ArgumentNullException("configuration");

            m_LoggerFactory = loggerFactory;
            m_Logger = logger;
            m_Configuration = configuration;

            initialize(m_Configuration);
        }

        public void Start()
        {
            copyDirectoriesFrom(
                Path.Combine(Path.GetFullPath("."), "content"),
                Path.Combine(Path.GetFullPath("."), "bin"));

            m_Node.StartAndWaitUntilReady();
        }

        private void copyDirectoriesFrom(String from, String to)
        {
            //Now Create all of the directories
            foreach (String dirPath in Directory.GetDirectories(from, "*",
                SearchOption.AllDirectories))
                Directory.CreateDirectory(dirPath.Replace(from, to));

            //Copy all the files & Replaces any files with the same name
            foreach (String newPath in Directory.GetFiles(from, "*.*",
                SearchOption.AllDirectories))
                File.Copy(newPath, newPath.Replace(from, to), true);

        }

        void IDisposable.Dispose()
        {
            if (m_Node != null)
                m_Node.Stop();
            
            if (m_DbLock != null && m_DbLock.IsAcquired)
                m_DbLock.Release();
        }

        private static int getQuorumSize(int clusterSize)
        {
            return clusterSize / 2 + 1;
        }

        private IAuthenticationProviderFactory getAuthenticationProviderFactory(String authenticationType, String authenticationConfigFile)
        {
            if ("internal".Equals(authenticationType))
                return new InternalAuthenticationProviderFactory();

            throw new Exception(String.Format("The authentication type {0} is not recognised.", authenticationType));

        }

        private void initialize(ClusterNodeConfiguration configuration)
        {
            Init(configuration.Defines);

            var dbPath = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, "event-store"));

            if (!configuration.InMemDb)
            {
                m_DbLock = new ExclusiveDbLock(dbPath);
                if (!m_DbLock.Acquire())
                    throw new Exception(String.Format("Couldn't acquire exclusive lock on DB at '{0}'.", dbPath));
            }
            m_ClusterNodeMutex = new ClusterNodeMutex();
            if (!m_ClusterNodeMutex.Acquire())
                throw new Exception(String.Format("Couldn't acquire exclusive Cluster Node mutex '{0}'.", m_ClusterNodeMutex.MutexName));
            
            var db = new TFChunkDb(CreateDbConfig(dbPath, configuration.CachedChunks, configuration.ChunksCacheSize, configuration.InMemDb));
            

            IGossipSeedSource gossipSeedSource;
            if (configuration.DiscoverViaDns)
            {
                gossipSeedSource = new DnsGossipSeedSource(configuration.ClusterDns, configuration.ClusterGossipPort);
            }
            else
            {
                if (configuration.GossipSeeds.Length == 0)
                {
                    if (configuration.ClusterSize > 1)
                    {
                        m_Logger.Error(String.Format("DNS discovery is disabled, but no gossip seed endpoints have been specified. " +
                                                "Specify gossip seeds using the 'GossipSeed' configuration property."));
                    }
                    else
                    {
                        m_Logger.Info(String.Format("DNS discovery is disabled, but no gossip seed endpoints have been specified. Since" +
                                               "the cluster size is set to 1, this may be intentional. Gossip seeds can be specified" +
                                               "seeds using the 'GossipSeed' configuration property."));
                    }
                }

                gossipSeedSource = new KnownEndpointGossipSeedSource(configuration.GossipSeedsEndPoints);
            }

            var runProjections = configuration.RunProjections;

            var enabledNodeSubsystems = runProjections >= ProjectionType.System
               ? new[] { NodeSubsystems.Projections }
               : new NodeSubsystems[0];
            m_Projections = new ProjectionsSubsystem(configuration.ProjectionThreads, configuration.RunProjections, configuration.StartStandardProjections);
            var infoController = new InfoController(configuration, configuration.RunProjections); 
            m_Node = BuildNode(configuration); //new ClusterVNode(db, nodeSettings, gossipSeedSource, infoController, m_Projections);

            RegisterWebControllers(enabledNodeSubsystems, configuration);
        }

        private static int GetQuorumSize(int clusterSize)
        {
            if (clusterSize == 1) return 1;
            return clusterSize / 2 + 1;
        }

        private ClusterVNode BuildNode(ClusterNodeConfiguration configuration)
        {
            var options = configuration;
            var quorumSize = GetQuorumSize(options.ClusterSize);

            var intHttp = new IPEndPoint(options.InternalIpAddress, options.InternalHttpPort);
            var extHttp = new IPEndPoint(options.ExternalIpAddress, options.ExternalHttpPort);
            var intTcp = new IPEndPoint(options.InternalIpAddress, options.InternalTcpPort);
            var intSecTcp = options.InternalSecureTcpPort > 0 ? new IPEndPoint(options.InternalIpAddress, options.InternalSecureTcpPort) : null;
            var extTcp = new IPEndPoint(options.ExternalIpAddress, options.ExternalTcpPort);
            var extSecTcp = options.ExternalSecureTcpPort > 0 ? new IPEndPoint(options.ExternalIpAddress, options.ExternalSecureTcpPort) : null;

            var prepareCount = options.PrepareCount > quorumSize ? options.PrepareCount : quorumSize;
            var commitCount = options.CommitCount > quorumSize ? options.CommitCount : quorumSize;
            m_Logger.Info("Quorum size set to " + prepareCount);
            if (options.UseInternalSsl)
            {
                if (ReferenceEquals(options.SslTargetHost, Opts.SslTargetHostDefault)) throw new Exception("No SSL target host specified.");
                if (intSecTcp == null) throw new Exception("Usage of internal secure communication is specified, but no internal secure endpoint is specified!");
            }

            VNodeBuilder builder;
            if (options.ClusterSize > 1)
            {
                builder = ClusterVNodeBuilder.AsClusterMember(options.ClusterSize);
            }
            else
            {
                builder = ClusterVNodeBuilder.AsSingleNode();
            }
            if (options.InMemDb)
            {
                builder = builder.RunInMemory();
            }
            else
            {
                var dbPath = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, "event-store"));
                builder = builder.RunOnDisk(dbPath);
            }

            var indexPath = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, "event-store-index"));
            builder.WithInternalTcpOn(intTcp)
                        .WithInternalSecureTcpOn(intSecTcp)
                        .WithExternalTcpOn(extTcp)
                        .WithExternalSecureTcpOn(extSecTcp)
                        .WithInternalHttpOn(intHttp)
                        .WithExternalHttpOn(extHttp)
                        .WithWorkerThreads(options.WorkerThreads)
                        .WithInternalHeartbeatTimeout(TimeSpan.FromMilliseconds(options.InternalTcpHeartbeatTimeout))
                        .WithInternalHeartbeatInterval(TimeSpan.FromMilliseconds(options.InternalTcpHeartbeatInterval))
                        .WithExternalHeartbeatTimeout(TimeSpan.FromMilliseconds(options.ExternalTcpHeartbeatTimeout))
                        .WithExternalHeartbeatInterval(TimeSpan.FromMilliseconds(options.ExternalTcpHeartbeatInterval))
                        .MaximumMemoryTableSizeOf(options.MaxMemTableSize)
                        .WithHashCollisionReadLimitOf(options.HashCollisionReadLimit)
                        .WithGossipInterval(TimeSpan.FromMilliseconds(options.GossipIntervalMs))
                        .WithGossipAllowedTimeDifference(TimeSpan.FromMilliseconds(options.GossipAllowedDifferenceMs))
                        .WithGossipTimeout(TimeSpan.FromMilliseconds(options.GossipTimeoutMs))
                        .WithClusterGossipPort(options.ClusterGossipPort)
                        .WithMinFlushDelay(TimeSpan.FromMilliseconds(options.MinFlushDelayMs))
                        .WithPrepareTimeout(TimeSpan.FromMilliseconds(options.PrepareTimeoutMs))
                        .WithCommitTimeout(TimeSpan.FromMilliseconds(options.CommitTimeoutMs))
                        .WithStatsPeriod(TimeSpan.FromSeconds(options.StatsPeriodSec))
                        .WithPrepareCount(prepareCount)
                        .WithCommitCount(commitCount)
                        .WithNodePriority(options.NodePriority)
                        .WithScavengeHistoryMaxAge(options.ScavengeHistoryMaxAge)
                        .WithIndexPath(indexPath)
                        .WithIndexCacheDepth(options.IndexCacheDepth)
                        .WithSslTargetHost(options.SslTargetHost)
                        .RunProjections(options.RunProjections, options.ProjectionThreads)
                        .WithTfCachedChunks(options.CachedChunks)
                        .WithTfChunksCacheSize(options.ChunksCacheSize)
                        .WithStatsStorage(StatsStorage.StreamAndCsv)
                        .AdvertiseInternalIPAs(options.InternalIpAddressAdvertiseAs)
                        .AdvertiseExternalIPAs(options.ExternalIpAddressAdvertiseAs)
                        .AdvertiseInternalHttpPortAs(options.InternalHttpPortAdvertiseAs)
                        .AdvertiseExternalHttpPortAs(options.ExternalHttpPortAdvertiseAs)
                        .AdvertiseInternalTCPPortAs(options.InternalTcpPortAdvertiseAs)
                        .AdvertiseExternalTCPPortAs(options.ExternalTcpPortAdvertiseAs)
                        .AdvertiseInternalSecureTCPPortAs(options.InternalSecureTcpPortAdvertiseAs)
                        .AdvertiseExternalSecureTCPPortAs(options.ExternalSecureTcpPortAdvertiseAs)
                        .HavingReaderThreads(options.ReaderThreadsCount)
                        ;

            if (options.GossipSeedsEndPoints.Length > 0)
                builder.WithGossipSeeds(options.GossipSeedsEndPoints);

            if (options.DiscoverViaDns)
                builder.WithClusterDnsName(options.ClusterDns);
            else
                builder.DisableDnsDiscovery();

            if (!options.AddInterfacePrefixes)
            {
                builder.DontAddInterfacePrefixes();
            }

            foreach (var prefix in options.InternalHttpPrefixes)
            {
                builder.AddInternalHttpPrefix(prefix);
            }
            foreach (var prefix in options.ExternalHttpPrefixes)
            {
                builder.AddExternalHttpPrefix(prefix);
            }

            if (options.EnableTrustedAuth)
                builder.EnableTrustedAuth();
            if (options.StartStandardProjections)
                builder.StartStandardProjections();
            if (options.DisableHTTPCaching)
                builder.DisableHTTPCaching();
            if (options.DisableScavengeMerging)
                builder.DisableScavengeMerging();
            if (options.LogHttpRequests)
                builder.EnableLoggingOfHttpRequests();
            if (options.EnableHistograms)
                builder.EnableHistograms();
            if (options.UnsafeIgnoreHardDelete)
                builder.WithUnsafeIgnoreHardDelete();
            if (options.UnsafeDisableFlushToDisk)
                builder.WithUnsafeDisableFlushToDisk();
            if (options.BetterOrdering)
                builder.WithBetterOrdering();
            if (options.SslValidateServer)
                builder.ValidateSslServer();
            if (options.UseInternalSsl)
                builder.EnableSsl();
            if (!options.AdminOnExt)
                builder.NoAdminOnPublicInterface();
            if (!options.StatsOnExt)
                builder.NoStatsOnPublicInterface();
            if (!options.GossipOnExt)
                builder.NoGossipOnPublicInterface();
            if (options.SkipDbVerify)
                builder.DoNotVerifyDbHashes();

            if (options.InternalSecureTcpPort > 0 || options.ExternalSecureTcpPort > 0)
            {
                if (!string.IsNullOrWhiteSpace(options.CertificateStoreLocation))
                {
                    var location = GetCertificateStoreLocation(options.CertificateStoreLocation);
                    var name = GetCertificateStoreName(options.CertificateStoreName);
                    builder.WithServerCertificateFromStore(location, name, options.CertificateSubjectName, options.CertificateThumbprint);
                }
                else if (!string.IsNullOrWhiteSpace(options.CertificateStoreName))
                {
                    var name = GetCertificateStoreName(options.CertificateStoreName);
                    builder.WithServerCertificateFromStore(name, options.CertificateSubjectName, options.CertificateThumbprint);
                }
                else if (options.CertificateFile.IsNotEmptyString())
                {
                    builder.WithServerCertificateFromFile(options.CertificateFile, options.CertificatePassword);
                }
                else
                    throw new Exception("No server certificate specified.");
            }

            var authenticationConfig = options.Config;
            var plugInContainer = FindPlugins();
            var authenticationProviderFactory = GetAuthenticationProviderFactory(options.AuthenticationType, authenticationConfig, plugInContainer);
            var consumerStrategyFactories = GetPlugInConsumerStrategyFactories(plugInContainer);
            builder.WithAuthenticationProvider(authenticationProviderFactory);

            return builder.Build(options, consumerStrategyFactories);
        }

        private IPersistentSubscriptionConsumerStrategyFactory[] GetPlugInConsumerStrategyFactories(CompositionContainer plugInContainer)
        {
            var allPlugins = plugInContainer.GetExports<IPersistentSubscriptionConsumerStrategyPlugin>();

            var strategyFactories = new List<IPersistentSubscriptionConsumerStrategyFactory>();

            foreach (var potentialPlugin in allPlugins)
            {
                try
                {
                    var plugin = potentialPlugin.Value;
                    m_Logger.InfoFormat("Loaded consumer strategy plugin: {0} version {1}.", plugin.Name, plugin.Version);
                    strategyFactories.Add(plugin.GetConsumerStrategyFactory());
                }
                catch (CompositionException ex)
                {
                    m_Logger.Error("Error loading consumer strategy plugin.", ex);
                }
            }

            return strategyFactories.ToArray();
        }

        private IAuthenticationProviderFactory GetAuthenticationProviderFactory(string authenticationType, string authenticationConfigFile, CompositionContainer plugInContainer)
        {
            var potentialPlugins = plugInContainer.GetExports<IAuthenticationPlugin>();

            var authenticationTypeToPlugin = new Dictionary<string, Func<IAuthenticationProviderFactory>> {
                { "internal", () => new InternalAuthenticationProviderFactory() }
            };

            foreach (var potentialPlugin in potentialPlugins)
            {
                try
                {
                    var plugin = potentialPlugin.Value;
                    var commandLine = plugin.CommandLineName.ToLowerInvariant();
                    m_Logger.InfoFormat("Loaded authentication plugin: {0} version {1} (Command Line: {2})", plugin.Name, plugin.Version, commandLine);
                    authenticationTypeToPlugin.Add(commandLine, () => plugin.GetAuthenticationProviderFactory(authenticationConfigFile));
                }
                catch (CompositionException ex)
                {
                    m_Logger.Error("Error loading authentication plugin.", ex);
                }
            }

            Func<IAuthenticationProviderFactory> factory;
            if (!authenticationTypeToPlugin.TryGetValue(authenticationType.ToLowerInvariant(), out factory))
            {
                throw new ApplicationInitializationException(string.Format("The authentication type {0} is not recognised. If this is supposed " +
                            "to be provided by an authentication plugin, confirm the plugin DLL is located in {1}.\n" +
                            "Valid options for authentication are: {2}.", authenticationType, Locations.PluginsDirectory, string.Join(", ", authenticationTypeToPlugin.Keys)));
            }

            return factory();
        }

        private CompositionContainer FindPlugins()
        {
            var catalog = new AggregateCatalog();

            catalog.Catalogs.Add(new AssemblyCatalog(typeof(ClusterNodeApplication).Assembly));

            if (Directory.Exists(Locations.PluginsDirectory))
            {
                m_Logger.InfoFormat("Plugins path: {0}", Locations.PluginsDirectory);
                catalog.Catalogs.Add(new DirectoryCatalog(Locations.PluginsDirectory));
            }
            else
            {
                m_Logger.InfoFormat("Cannot find plugins path: {0}", Locations.PluginsDirectory);
            }

            return new CompositionContainer(catalog);
        }

        private void RegisterWebControllers(NodeSubsystems[] enabledNodeSubsystems, ClusterNodeConfiguration settings)
        {
            if (m_Node.InternalHttpService != null)
            {
                m_Node.InternalHttpService.SetupController(new ClusterWebUiController(m_Node.MainQueue, enabledNodeSubsystems));
            }
            if (settings.AdminOnExt)
            {
                m_Node.ExternalHttpService.SetupController(new ClusterWebUiController(m_Node.MainQueue, enabledNodeSubsystems));
            }
        }

        protected void Init(String[] defines)
        {
            Application.AddDefines(defines); 

            LogManager.SetLogFactory(s => new CastleToEventStoreLoggerAdapter(m_LoggerFactory.Create(s)));
        }

        protected static TFChunkDbConfig CreateDbConfig(String dbPath, int cachedChunks, long chunksCacheSize, bool inMemDb)
        {
            if (!Directory.Exists(dbPath))
                Directory.CreateDirectory(dbPath);

            ICheckpoint writerChk;
            ICheckpoint chaserChk;
            ICheckpoint epochChk;
            ICheckpoint truncateChk;

            if (inMemDb)
            {
                writerChk = new InMemoryCheckpoint(Checkpoint.Writer);
                chaserChk = new InMemoryCheckpoint(Checkpoint.Chaser);
                epochChk = new InMemoryCheckpoint(Checkpoint.Epoch, -1);
                truncateChk = new InMemoryCheckpoint(Checkpoint.Truncate, -1);
            }
            else
            {
               

                var writerCheckFilename = Path.Combine(dbPath, Checkpoint.Writer + ".chk");
                var chaserCheckFilename = Path.Combine(dbPath, Checkpoint.Chaser + ".chk");
                var epochCheckFilename = Path.Combine(dbPath, Checkpoint.Epoch + ".chk");
                var truncateCheckFilename = Path.Combine(dbPath, Checkpoint.Truncate + ".chk");
                writerChk = new MemoryMappedFileCheckpoint(writerCheckFilename, Checkpoint.Writer, cached: true);
                chaserChk = new MemoryMappedFileCheckpoint(chaserCheckFilename, Checkpoint.Chaser, cached: true);
                epochChk = new MemoryMappedFileCheckpoint(epochCheckFilename, Checkpoint.Epoch, cached: true,
                                                          initValue: -1);
                truncateChk = new MemoryMappedFileCheckpoint(truncateCheckFilename, Checkpoint.Truncate, cached: true,
                                                             initValue: -1);
            }
            var cache = cachedChunks >= 0
                            ? cachedChunks * (long)(TFConsts.ChunkSize + ChunkHeader.Size + ChunkFooter.Size)
                            : chunksCacheSize;
            var nodeConfig = new TFChunkDbConfig(dbPath,
                                                 new VersionedPatternFileNamingStrategy(dbPath, "chunk-"),
                                                 TFConsts.ChunkSize,                                                 
                                                 cache,
                                                 writerChk,
                                                 chaserChk,
                                                 epochChk,
                                                 truncateChk);
            return nodeConfig;
        }

        protected static StoreLocation GetCertificateStoreLocation(string certificateStoreLocation)
        {
            StoreLocation location;
            if (!Enum.TryParse(certificateStoreLocation, out location))
                throw new Exception(string.Format("Could not find certificate store location '{0}'", certificateStoreLocation));
            return location;
        }

        protected static StoreName GetCertificateStoreName(string certificateStoreName)
        {
            StoreName name;
            if (!Enum.TryParse(certificateStoreName, out name))
                throw new Exception(string.Format("Could not find certificate store name '{0}'", certificateStoreName));
            return name;
        }

        protected static X509Certificate2 LoadCertificateFromFile(String path, String password)
        {
            return new X509Certificate2(path, password);
        }

        protected static X509Certificate2 LoadCertificateFromStore(String certificateStoreLocation, String certificateStoreName, String certificateSubjectName, String certificateThumbprint)
        {
            X509Store store;

            if (!String.IsNullOrWhiteSpace(certificateStoreLocation))
            {
                StoreLocation location;
                if (!Enum.TryParse(certificateStoreLocation, out location))
                    throw new Exception(String.Format("Could not find certificate store location '{0}'", certificateStoreLocation));

                StoreName name;
                if (!Enum.TryParse(certificateStoreName, out name))
                    throw new Exception(String.Format("Could not find certificate store name '{0}'", certificateStoreName));

                store = new X509Store(name, location);

                try
                {
                    store.Open(OpenFlags.OpenExistingOnly);
                }
                catch (Exception exc)
                {
                    throw new Exception(String.Format("Could not open certificate store '{0}' in location {1}'.", name, location), exc);
                }
            }
            else
            {
                StoreName name;
                if (!Enum.TryParse(certificateStoreName, out name))
                    throw new Exception(String.Format("Could not find certificate store name '{0}'", certificateStoreName));

                store = new X509Store(name);

                try
                {
                    store.Open(OpenFlags.OpenExistingOnly);
                }
                catch (Exception exc)
                {
                    throw new Exception(String.Format("Could not open certificate store '{0}'.", name), exc);
                }
            }

            if (!String.IsNullOrWhiteSpace(certificateThumbprint))
            {
                var certificates = store.Certificates.Find(X509FindType.FindByThumbprint, certificateThumbprint, true);
                if (certificates.Count == 0)
                    throw new Exception(String.Format("Could not find valid certificate with thumbprint '{0}'.", certificateThumbprint));

                //Can this even happen?
                if (certificates.Count > 1)
                    throw new Exception(String.Format("Could not determine a unique certificate from thumbprint '{0}'.", certificateThumbprint));

                return certificates[0];
            }

            if (!String.IsNullOrWhiteSpace(certificateSubjectName))
            {
                var certificates = store.Certificates.Find(X509FindType.FindBySubjectName, certificateSubjectName, true);
                if (certificates.Count == 0)
                    throw new Exception(String.Format("Could not find valid certificate with thumbprint '{0}'.", certificateThumbprint));

                //Can this even happen?
                if (certificates.Count > 1)
                    throw new Exception(String.Format("Could not determine a unique certificate from thumbprint '{0}'.", certificateThumbprint));

                return certificates[0];
            }

            throw new ArgumentException("No thumbprint or subject name was specified for a certificate, but a certificate store was specified.");
        }

        private static IPAddress GetNonLoopbackAddress()
        {
            foreach (var adapter in NetworkInterface.GetAllNetworkInterfaces())
            {
                foreach (UnicastIPAddressInformation address in adapter.GetIPProperties().UnicastAddresses)
                {
                    if (address.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    {
                        if (!IPAddress.IsLoopback(address.Address))
                        {
                            return address.Address;
                        }
                    }
                }
            }
            return null;
        }
    }
}
