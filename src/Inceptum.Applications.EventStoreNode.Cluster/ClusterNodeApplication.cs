using System;
using System.IO;
using System.Net;
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
using Inceptum.Applications.EventStoreNode.Common;

namespace Inceptum.Applications.EventStoreNode.Cluster
{
    public class ClusterNodeApplication : NodeApplicationBase, IHostedApplication, IDisposable
    {
        private readonly ILogger m_Logger;
        private readonly ClusterNodeConfiguration m_Configuration;
        
        private ExclusiveDbLock m_DbLock;
        private ProjectionsSubsystem m_Projections;
        private ClusterVNode m_Node;
        private ClusterNodeMutex m_ClusterNodeMutex;
        
        public ClusterNodeApplication(ILoggerFactory loggerFactory, ILogger logger, ClusterNodeConfiguration configuration) : base(loggerFactory)
        {
            m_Logger = logger;
            m_Configuration = configuration;

            initialize(m_Configuration);
        }

        public void Start()
        {
            m_Node.Start();
        }

        public void Dispose()
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
            
            X509Certificate2 certificate = null;
            if (configuration.InternalSecureTcpPort > 0 || configuration.ExternalSecureTcpPort > 0)
            {
                if (configuration.CertificateStore.IsNotEmptyString())
                    certificate = LoadCertificateFromStore(configuration.CertificateStore, configuration.CertificateName);
                else if (configuration.CertificateFile.IsNotEmptyString())
                    certificate = LoadCertificateFromFile(configuration.CertificateFile, configuration.CertificatePassword);
                else
                    throw new Exception("No server certificate specified.");
            }

            var intHttp = new IPEndPoint(configuration.InternalIpAddress, configuration.InternalHttpPort);
            var extHttp = new IPEndPoint(configuration.ExternalIpAddress, configuration.ExternalHttpPort);
            var intTcp = new IPEndPoint(configuration.InternalIpAddress, configuration.InternalTcpPort);
            var intSecTcp = configuration.InternalSecureTcpPort > 0 ? new IPEndPoint(configuration.InternalIpAddress, configuration.InternalSecureTcpPort) : null;
            var extTcp = new IPEndPoint(configuration.ExternalIpAddress, configuration.ExternalTcpPort);
            var extSecTcp = configuration.ExternalSecureTcpPort > 0 ? new IPEndPoint(configuration.ExternalIpAddress, configuration.ExternalSecureTcpPort) : null;
            var prefixes = configuration.HttpPrefixes.IsNotEmpty() ? configuration.HttpPrefixes : new[] { extHttp.ToHttpUrl() };
            var quorumSize = getQuorumSize(configuration.ClusterSize);
            var prepareCount = configuration.PrepareCount > quorumSize ? configuration.PrepareCount : quorumSize;
            var commitCount = configuration.CommitCount > quorumSize ? configuration.CommitCount : quorumSize;
            
            m_Logger.Info("Quorum size set to " + prepareCount);

            if (configuration.UseInternalSsl)
            {
                if (ReferenceEquals(configuration.SslTargetHost, Opts.SslTargetHostDefault)) throw new Exception("No SSL target host specified.");
                if (intSecTcp == null) throw new Exception("Usage of internal secure communication is specified, but no internal secure endpoint is specified!");
            }

            var authenticationProviderFactory = getAuthenticationProviderFactory(configuration.AuthenticationType, configuration.AuthenticationConfigFile);

            var nodeSettings =  new ClusterVNodeSettings(Guid.NewGuid(),
	                                        intTcp, intSecTcp, extTcp, extSecTcp, intHttp, extHttp,
	                                        prefixes, configuration.EnableTrustedAuth,
	                                        certificate,
	                                        configuration.WorkerThreads, configuration.DiscoverViaDns,
	                                        configuration.ClusterDns, configuration.GossipSeedsEndPoints,
											TimeSpan.FromMilliseconds(configuration.MinFlushDelayMs), configuration.ClusterSize,
	                                        prepareCount, commitCount,
	                                        TimeSpan.FromMilliseconds(configuration.PrepareTimeoutMs),
	                                        TimeSpan.FromMilliseconds(configuration.CommitTimeoutMs),
	                                        configuration.UseInternalSsl, configuration.SslTargetHost, configuration.SslValidateServer,
	                                        TimeSpan.FromSeconds(configuration.StatsPeriodSec), StatsStorage.StreamAndCsv,
											configuration.NodePriority, authenticationProviderFactory, configuration.DisableScavengeMerging);

            
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

            var dbVerifyHashes = !configuration.SkipDbVerify;
            var runProjections = configuration.RunProjections;

            var enabledNodeSubsystems = runProjections >= RunProjections.System
               ? new[] { NodeSubsystems.Projections }
               : new NodeSubsystems[0];
            m_Projections = new ProjectionsSubsystem(configuration.ProjectionThreads, configuration.RunProjections);
            m_Node = new ClusterVNode(db, nodeSettings, gossipSeedSource, dbVerifyHashes, ESConsts.MemTableEntryCount, m_Projections);
            m_Node.MainBus.Subscribe(new UserManagementProjectionsRegistration());
            m_Node.InternalHttpService.SetupController(new ClusterWebUIController(m_Node.MainQueue, enabledNodeSubsystems));
            m_Node.ExternalHttpService.SetupController(new ClusterWebUIController(m_Node.MainQueue, enabledNodeSubsystems));
            m_Node.InternalHttpService.SetupController(new UsersWebController(m_Node.MainQueue));
            m_Node.ExternalHttpService.SetupController(new UsersWebController(m_Node.MainQueue));
        }
    }
}
