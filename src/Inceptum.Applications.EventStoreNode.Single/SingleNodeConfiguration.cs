using System.Net;
using EventStore.Common.Options;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.Util;
using Newtonsoft.Json;

namespace Inceptum.Applications.EventStoreNode.Single
{
    public class SingleNodeConfiguration
    {
        public SingleNodeConfiguration()
        {
            Defines = Opts.DefinesDefault;
            CachedChunks = Opts.CachedChunksDefault;
            ChunksCacheSize = Opts.ChunksCacheSizeDefault;
            IpAddress = Opts.IpDefault;
            TcpPort = Opts.TcpPortDefault;
            SecureTcpPort = Opts.SecureTcpPortDefault;
            HttpPort = Opts.HttpPortDefault;
            DbPath = Opts.DbPathDefault;
            InMemDb = Opts.InMemDbDefault;
            SkipDbVerify = Opts.SkipDbVerifyDefault;
            RunProjections = Opts.RunProjectionsDefault;
            ProjectionThreads = Opts.ProjectionThreadsDefault;
            WorkerThreads = Opts.WorkerThreadsDefault;
            DisableScavengeMerging = Opts.DisableScavengeMergeDefault;
            HttpPrefixes = Opts.HttpPrefixesDefault;
            EnableTrustedAuth = Opts.EnableTrustedAuthDefault;
            CertificateStore = Opts.CertificateStoreDefault;
            CertificateName = Opts.CertificateNameDefault;
            CertificateFile = Opts.CertificateFileDefault;
            CertificatePassword = Opts.CertificatePasswordDefault;
            MinFlushDelayMs = Opts.MinFlushDelayMsDefault;
            PrepareTimeoutMs = Opts.PrepareTimeoutMsDefault;
            CommitTimeoutMs = Opts.CommitTimeoutMsDefault;
            StatsPeriodSec = Opts.StatsPeriodDefault;
        }

        public string[] Defines { get; set; }

        public int CachedChunks { get; set; }
        public long ChunksCacheSize { get; set; }
        [JsonIgnore]
        public IPAddress IpAddress { get; set; }

        public string Ip
        {
            get { return IpAddress.ToString(); }
            set { IpAddress = IPAddress.Parse(value); }
        }

        public int TcpPort { get; set; }
        public int SecureTcpPort { get; set; }
        public int HttpPort { get; set; }

        public string DbPath { get; set; }
        public bool InMemDb { get; set; }
        public bool SkipDbVerify { get; set; }
        public RunProjections RunProjections { get; set; }
        public int ProjectionThreads { get; set; }
        public int WorkerThreads { get; set; }

        public bool DisableScavengeMerging { get; set; }

        public string[] HttpPrefixes { get; set; }
        public bool EnableTrustedAuth { get; set; }

        public string CertificateStore { get; set; }
        public string CertificateName { get; set; }
        public string CertificateFile { get; set; }
        public string CertificatePassword { get; set; }

        public double MinFlushDelayMs { get; set; }

        public int PrepareTimeoutMs { get; set; }
        public int CommitTimeoutMs { get; set; }

        public int StatsPeriodSec { get; set; }
    }
}