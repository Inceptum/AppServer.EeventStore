using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Castle.Core.Logging;
using EventStore.Common.Options;
using EventStore.Common.Utils;
using EventStore.Core;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Services.Transport.Http.Controllers;
using EventStore.Core.Settings;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Projections.Core;
using EventStore.Web.Users;
using Inceptum.AppServer;
using Inceptum.Applications.EventStoreNode.Common;

namespace Inceptum.Applications.EventStoreNode.Single
{
    public class SingleNodeApplication
        : NodeApplicationBase, IHostedApplication, IDisposable
    {
        private readonly SingleNodeConfiguration m_Configuration;
        private ExclusiveDbLock m_DbLock;
        private ProjectionsSubsystem m_Projections;
        private SingleVNode m_Node;

        public SingleNodeApplication(ILoggerFactory loggerFactory, SingleNodeConfiguration configuration) 
            : base(loggerFactory)
        {
            m_Configuration = configuration;

            initialize(m_Configuration);
        }

        void IHostedApplication.Start()
        {
            m_Node.Start();
        }

        public void Dispose()
        {
            if (m_Node != null)
                m_Node.Stop(false);
            
            if (m_DbLock != null && m_DbLock.IsAcquired)
                m_DbLock.Release();
        }

        void initialize(SingleNodeConfiguration configuration)
        {
            Init(configuration.Defines);

            var dbPath = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, "event-store"));

            if (configuration.InMemDb)
            {
                m_DbLock = new ExclusiveDbLock(dbPath);
                if (!m_DbLock.Acquire())
                    throw new Exception(string.Format("Couldn't acquire exclusive lock on DB at '{0}'.", dbPath));
            }
            var db = new TFChunkDb(CreateDbConfig(dbPath, configuration.CachedChunks, configuration.ChunksCacheSize, configuration.InMemDb));

            X509Certificate2 certificate = null;
            if (configuration.SecureTcpPort > 0)
            {
                if (configuration.CertificateStore.IsNotEmptyString())
                    certificate = LoadCertificateFromStore(configuration.CertificateStore, configuration.CertificateName);
                else if (configuration.CertificateFile.IsNotEmptyString())
                    certificate = LoadCertificateFromFile(configuration.CertificateFile, configuration.CertificatePassword);
                else
                    throw new Exception("No server certificate specified.");
            }
            var tcpEndPoint = new IPEndPoint(configuration.IpAddress, configuration.TcpPort);
            var secureTcpEndPoint = configuration.SecureTcpPort > 0 ? new IPEndPoint(configuration.IpAddress, configuration.SecureTcpPort) : null;
            var httpEndPoint = new IPEndPoint(configuration.IpAddress, configuration.HttpPort);
            var prefixes = configuration.HttpPrefixes.IsNotEmpty() ? configuration.HttpPrefixes : new[] { httpEndPoint.ToHttpUrl() };
            var vnodeSettings = new SingleVNodeSettings(tcpEndPoint,
                                                        secureTcpEndPoint,
                                                        httpEndPoint,
                                                        prefixes.Select(p => p.Trim()).ToArray(),
                                                        configuration.EnableTrustedAuth,
                                                        certificate,
                                                        configuration.WorkerThreads, 
                                                        TimeSpan.FromMilliseconds(configuration.MinFlushDelayMs),
                                                        TimeSpan.FromMilliseconds(configuration.PrepareTimeoutMs),
                                                        TimeSpan.FromMilliseconds(configuration.CommitTimeoutMs),
                                                        TimeSpan.FromSeconds(configuration.StatsPeriodSec),
                                                        StatsStorage.StreamAndCsv,
                                                        false,
                                                        configuration.DisableScavengeMerging);
            
            var dbVerifyHashes = !configuration.SkipDbVerify;
            var runProjections = configuration.RunProjections;
            
            var enabledNodeSubsystems = runProjections >= RunProjections.System
                ? new[] { NodeSubsystems.Projections }
                : new NodeSubsystems[0];
            m_Projections = new ProjectionsSubsystem(configuration.ProjectionThreads, runProjections);
            m_Node = new SingleVNode(db, vnodeSettings, dbVerifyHashes, ESConsts.MemTableEntryCount, m_Projections);
            
            m_Node.HttpService.SetupController(new WebSiteController(m_Node.MainQueue, enabledNodeSubsystems));
            m_Node.HttpService.SetupController(new UsersWebController(m_Node.MainQueue));
            var users = new UserManagementProjectionsRegistration();
            m_Node.MainBus.Subscribe(users);
        }
        
    }
}
