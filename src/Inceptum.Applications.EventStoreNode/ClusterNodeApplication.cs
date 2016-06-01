using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Castle.Core.Logging;
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
using EventStore.Web.Users;
using Inceptum.AppServer;
using EventStore.Common.Log;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.FileNamingStrategy;


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
            if (m_Logger.IsDebugEnabled)
            {
                m_Logger.DebugFormat("ApplicationDirectory {0}", Locations.ApplicationDirectory);
                m_Logger.DebugFormat("ApplicationDirectory1 {0}", Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
                m_Logger.DebugFormat("ApplicationDirectory2 {0}", Path.GetFullPath("."));
            }

            m_Node.StartAndWaitUntilReady();
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

        private IAuthenticationProviderFactory getAuthenticationProviderFactory(string authenticationType, string authenticationConfigFile)
        {
            if ("internal".Equals(authenticationType))
                return new InternalAuthenticationProviderFactory();

            throw new Exception(string.Format("The authentication type {0} is not recognised.", authenticationType));

        }

        private void initialize(ClusterNodeConfiguration configuration)
        {
            Init(configuration.Defines);

            var dbPath = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, "event-store"));

            if (!configuration.InMemDb)
            {
                m_DbLock = new ExclusiveDbLock(dbPath);
                if (!m_DbLock.Acquire())
                    throw new Exception(string.Format("Couldn't acquire exclusive lock on DB at '{0}'.", dbPath));
            }
            m_ClusterNodeMutex = new ClusterNodeMutex();
            if (!m_ClusterNodeMutex.Acquire())
                throw new Exception(string.Format("Couldn't acquire exclusive Cluster Node mutex '{0}'.", m_ClusterNodeMutex.MutexName));
            
            var db = new TFChunkDb(CreateDbConfig(dbPath, configuration.CachedChunks, configuration.ChunksCacheSize, configuration.InMemDb));
            
            var nodeSettings = getClusterVNodeSettings(configuration);

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
                        m_Logger.Error(string.Format("DNS discovery is disabled, but no gossip seed endpoints have been specified. " +
                                                "Specify gossip seeds using the 'GossipSeed' configuration property."));
                    }
                    else
                    {
                        m_Logger.Info(string.Format("DNS discovery is disabled, but no gossip seed endpoints have been specified. Since" +
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
            m_Projections = new ProjectionsSubsystem(configuration.ProjectionThreads, configuration.RunProjections, Opts.StartStandardProjectionsDefault);
            var infoController = new InfoController(configuration, configuration.RunProjections); 
            m_Node = new ClusterVNode(db, nodeSettings, gossipSeedSource, infoController, m_Projections);

            RegisterWebControllers(enabledNodeSubsystems, nodeSettings);
        }

        private void RegisterWebControllers(NodeSubsystems[] enabledNodeSubsystems, ClusterVNodeSettings settings)
        {
            if (m_Node.InternalHttpService != null)
            {
                m_Node.InternalHttpService.SetupController(new ClusterWebUiController(m_Node.MainQueue, enabledNodeSubsystems));
            }
            if (settings.AdminOnPublic)
            {
                m_Node.ExternalHttpService.SetupController(new ClusterWebUiController(m_Node.MainQueue, enabledNodeSubsystems));
            }
        }


        private ClusterVNodeSettings getClusterVNodeSettings(ClusterNodeConfiguration configuration)
        {
            X509Certificate2 certificate = null;
            if (configuration.InternalSecureTcpPort > 0 || configuration.ExternalSecureTcpPort > 0)
            {
                if (configuration.CertificateStoreName.IsNotEmptyString())
                    certificate = LoadCertificateFromStore(configuration.CertificateStoreLocation, configuration.CertificateStoreName, configuration.CertificateSubjectName, configuration.CertificateThumbprint);
                else if (configuration.CertificateFile.IsNotEmptyString())
                    certificate = LoadCertificateFromFile(configuration.CertificateFile, configuration.CertificatePassword);
                else
                    throw new Exception("No server certificate specified.");
            }

            var intHttp = new IPEndPoint(configuration.InternalIpAddress, configuration.InternalHttpPort);
            var extHttp = new IPEndPoint(configuration.ExternalIpAddress, configuration.ExternalHttpPort);
            var intTcp = new IPEndPoint(configuration.InternalIpAddress, configuration.InternalTcpPort);
            var intSecTcp = configuration.InternalSecureTcpPort > 0
                ? new IPEndPoint(configuration.InternalIpAddress, configuration.InternalSecureTcpPort)
                : null;
            var extTcp = new IPEndPoint(configuration.ExternalIpAddress, configuration.ExternalTcpPort);
            var extSecTcp = configuration.ExternalSecureTcpPort > 0
                ? new IPEndPoint(configuration.ExternalIpAddress, configuration.ExternalSecureTcpPort)
                : null;
            var internalHttpPrefixes = configuration.InternalHttpPrefixes.IsNotEmpty()
                ? configuration.InternalHttpPrefixes
                : new string[0];
            var externalHttpPrefixes = configuration.ExternalHttpPrefixes.IsNotEmpty()
                ? configuration.ExternalHttpPrefixes
                : new string[0];
            var quorumSize = getQuorumSize(configuration.ClusterSize);

            GossipAdvertiseInfo gossipAdvertiseInfo;

            IPAddress intIpAddressToAdvertise = configuration.InternalIpAddressAdvertiseAs ?? configuration.InternalIpAddress;
            IPAddress extIpAddressToAdvertise = configuration.ExternalIpAddressAdvertiseAs ?? configuration.ExternalIpAddress;
            var additionalIntHttpPrefixes = new List<string>(internalHttpPrefixes);
            var additionalExtHttpPrefixes = new List<string>(externalHttpPrefixes);

            if ((configuration.InternalIpAddress.Equals(IPAddress.Parse("0.0.0.0")) ||
                configuration.ExternalIpAddress.Equals(IPAddress.Parse("0.0.0.0"))) && configuration.AddInterfacePrefixes)
            {
                IPAddress nonLoopbackAddress = GetNonLoopbackAddress();
                IPAddress addressToAdvertise = configuration.ClusterSize > 1 ? nonLoopbackAddress : IPAddress.Loopback;

                if (configuration.InternalIpAddress.Equals(IPAddress.Parse("0.0.0.0")))
                {
                    intIpAddressToAdvertise = configuration.InternalIpAddressAdvertiseAs ?? addressToAdvertise;
                    additionalIntHttpPrefixes.Add(String.Format("http://*:{0}/", intHttp.Port));
                }
                if (configuration.ExternalIpAddress.Equals(IPAddress.Parse("0.0.0.0")))
                {
                    extIpAddressToAdvertise = configuration.InternalIpAddressAdvertiseAs ?? addressToAdvertise;
                    additionalExtHttpPrefixes.Add(String.Format("http://*:{0}/", extHttp.Port));
                }
            }
            else if (configuration.AddInterfacePrefixes)
            {
                additionalIntHttpPrefixes.Add(String.Format("http://{0}:{1}/", configuration.InternalIpAddress, configuration.InternalHttpPort));
                if (configuration.InternalIpAddress.Equals(IPAddress.Loopback))
                {
                    additionalIntHttpPrefixes.Add(String.Format("http://localhost:{0}/", configuration.InternalHttpPort));
                }
                additionalExtHttpPrefixes.Add(String.Format("http://{0}:{1}/", configuration.ExternalIpAddress, configuration.ExternalHttpPort));
                if (configuration.ExternalIpAddress.Equals(IPAddress.Loopback))
                {
                    additionalExtHttpPrefixes.Add(String.Format("http://localhost:{0}/", configuration.ExternalHttpPort));
                }
            }

            internalHttpPrefixes = additionalIntHttpPrefixes.ToArray();
            externalHttpPrefixes = additionalExtHttpPrefixes.ToArray();


            var intTcpPort = configuration.InternalTcpPortAddressAdvertiseAs > 0 ? configuration.InternalTcpPortAddressAdvertiseAs : configuration.InternalTcpPort;
            var intTcpEndPoint = new IPEndPoint(intIpAddressToAdvertise, intTcpPort);
            var intSecureTcpPort = configuration.InternalSecureTcpPortAdvertiseAs > 0 ? configuration.InternalSecureTcpPortAdvertiseAs : configuration.InternalSecureTcpPort;
            var intSecureTcpEndPoint = new IPEndPoint(intIpAddressToAdvertise, intSecureTcpPort);

            var extTcpPort = configuration.ExternalTcpPortAdvertiseAs > 0 ? configuration.ExternalTcpPortAdvertiseAs : configuration.ExternalTcpPort;
            var extTcpEndPoint = new IPEndPoint(extIpAddressToAdvertise, extTcpPort);
            var extSecureTcpPort = configuration.ExternalSecureTcpPortAdvertiseAs > 0 ? configuration.ExternalSecureTcpPortAdvertiseAs : configuration.ExternalSecureTcpPort;
            var extSecureTcpEndPoint = new IPEndPoint(extIpAddressToAdvertise, extSecureTcpPort);

            var intHttpPort = configuration.InternalHttpPortAdvertiseAs > 0 ? configuration.InternalHttpPortAdvertiseAs : configuration.InternalHttpPort;
            var extHttpPort = configuration.ExternalHttpPortAdvertiseAs > 0 ? configuration.ExternalHttpPortAdvertiseAs : configuration.ExternalHttpPort;

            var intHttpEndPoint = new IPEndPoint(intIpAddressToAdvertise, intHttpPort);
            var extHttpEndPoint = new IPEndPoint(extIpAddressToAdvertise, extHttpPort);

            gossipAdvertiseInfo = new GossipAdvertiseInfo(intTcpEndPoint, intSecureTcpEndPoint,
                                                          extTcpEndPoint, extSecureTcpEndPoint,
                                                          intHttpEndPoint, extHttpEndPoint);

            var prepareCount = configuration.PrepareCount > quorumSize ? configuration.PrepareCount : quorumSize;
            var commitCount = configuration.CommitCount > quorumSize ? configuration.CommitCount : quorumSize;

            m_Logger.Info("Quorum size set to " + prepareCount);

            if (configuration.UseInternalSsl)
            {
                if (ReferenceEquals(configuration.SslTargetHost, Opts.SslTargetHostDefault))
                    throw new Exception("No SSL target host specified.");
                if (intSecTcp == null)
                    throw new Exception(
                        "Usage of internal secure communication is specified, but no internal secure endpoint is specified!");
            }

            var authenticationProviderFactory = getAuthenticationProviderFactory(configuration.AuthenticationType,
                configuration.AuthenticationConfigFile);

            var nodeSettings = new ClusterVNodeSettings(Guid.NewGuid(), 0,
                intTcp, intSecTcp, extTcp, extSecTcp, intHttp, extHttp, gossipAdvertiseInfo,
                internalHttpPrefixes, externalHttpPrefixes, configuration.EnableTrustedAuth,
                certificate,
                configuration.WorkerThreads, configuration.DiscoverViaDns,
                configuration.ClusterDns, configuration.GossipSeedsEndPoints,
                TimeSpan.FromMilliseconds(configuration.MinFlushDelayMs), configuration.ClusterSize,
                prepareCount, commitCount,
                TimeSpan.FromMilliseconds(configuration.PrepareTimeoutMs),
                TimeSpan.FromMilliseconds(configuration.CommitTimeoutMs),
                configuration.UseInternalSsl, configuration.SslTargetHost, configuration.SslValidateServer,
                TimeSpan.FromSeconds(configuration.StatsPeriodSec), StatsStorage.StreamAndCsv,
                configuration.NodePriority, authenticationProviderFactory, configuration.DisableScavengeMerging,
                configuration.ScavengeHistoryMaxAge,
                configuration.AdminOnExt, configuration.StatsOnExt, configuration.GossipOnExt,

                //TODO[MT]: below default values should be avaliable for configuration vua configuration class
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.GossipIntervalMsDefault),
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.GossipAllowedDifferenceMsDefault),
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.GossipTimeoutMsDefault),
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.IntTcpHeartbeatTimeoutDefault),
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.IntTcpHeartbeatInvervalDefault),
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.ExtTcpHeartbeatTimeoutDefault),
                TimeSpan.FromMilliseconds(EventStore.Core.Util.Opts.ExtTcpHeartbeatIntervalDefault),
                !EventStore.Core.Util.Opts.SkipDbVerifyDefault,
                EventStore.Core.Util.Opts.MaxMemtableSizeDefault,
                EventStore.Core.Util.Opts.StartStandardProjectionsDefault,
                EventStore.Core.Util.Opts.DisableHttpCachingDefault,
                EventStore.Core.Util.Opts.LogHttpRequestsDefault,
                index: null,
                enableHistograms: false,
                indexCacheDepth: 16,
                additionalConsumerStrategies: null,
                unsafeIgnoreHardDeletes: false,
                betterOrdering: false
                );
            return nodeSettings;
        }

        protected void Init(string[] defines)
        {
            Application.AddDefines(defines); 

            LogManager.SetLogFactory(s => new CastleToEventStoreLoggerAdapter(m_LoggerFactory.Create(s)));
        }

        protected static TFChunkDbConfig CreateDbConfig(string dbPath, int cachedChunks, long chunksCacheSize, bool inMemDb)
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

        protected static X509Certificate2 LoadCertificateFromFile(string path, string password)
        {
            return new X509Certificate2(path, password);
        }

        protected static X509Certificate2 LoadCertificateFromStore(string certificateStoreLocation, string certificateStoreName, string certificateSubjectName, string certificateThumbprint)
        {
            X509Store store;

            if (!string.IsNullOrWhiteSpace(certificateStoreLocation))
            {
                StoreLocation location;
                if (!Enum.TryParse(certificateStoreLocation, out location))
                    throw new Exception(string.Format("Could not find certificate store location '{0}'", certificateStoreLocation));

                StoreName name;
                if (!Enum.TryParse(certificateStoreName, out name))
                    throw new Exception(string.Format("Could not find certificate store name '{0}'", certificateStoreName));

                store = new X509Store(name, location);

                try
                {
                    store.Open(OpenFlags.OpenExistingOnly);
                }
                catch (Exception exc)
                {
                    throw new Exception(string.Format("Could not open certificate store '{0}' in location {1}'.", name, location), exc);
                }
            }
            else
            {
                StoreName name;
                if (!Enum.TryParse(certificateStoreName, out name))
                    throw new Exception(string.Format("Could not find certificate store name '{0}'", certificateStoreName));

                store = new X509Store(name);

                try
                {
                    store.Open(OpenFlags.OpenExistingOnly);
                }
                catch (Exception exc)
                {
                    throw new Exception(string.Format("Could not open certificate store '{0}'.", name), exc);
                }
            }

            if (!string.IsNullOrWhiteSpace(certificateThumbprint))
            {
                var certificates = store.Certificates.Find(X509FindType.FindByThumbprint, certificateThumbprint, true);
                if (certificates.Count == 0)
                    throw new Exception(string.Format("Could not find valid certificate with thumbprint '{0}'.", certificateThumbprint));

                //Can this even happen?
                if (certificates.Count > 1)
                    throw new Exception(string.Format("Could not determine a unique certificate from thumbprint '{0}'.", certificateThumbprint));

                return certificates[0];
            }

            if (!string.IsNullOrWhiteSpace(certificateSubjectName))
            {
                var certificates = store.Certificates.Find(X509FindType.FindBySubjectName, certificateSubjectName, true);
                if (certificates.Count == 0)
                    throw new Exception(string.Format("Could not find valid certificate with thumbprint '{0}'.", certificateThumbprint));

                //Can this even happen?
                if (certificates.Count > 1)
                    throw new Exception(string.Format("Could not determine a unique certificate from thumbprint '{0}'.", certificateThumbprint));

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
