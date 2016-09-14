using System;
using System.Globalization;
using System.Linq;
using System.Net;
using EventStore.Common.Options;
using EventStore.Common.Utils;
using EventStore.Core.Util;
using Newtonsoft.Json;

namespace Inceptum.Applications.EventStoreNode.Cluster
{
    public class ClusterNodeConfiguration : IOptions
    {
        private const bool m_Help = Opts.ShowHelpDefault;
        private const bool m_Version = Opts.ShowVersionDefault;
        private readonly string m_Config = string.Empty;
        private readonly string m_Log = Locations.DefaultLogDirectory;
        private const bool m_Force = Opts.ForceDefault;
        private const bool m_WhatIf = Opts.WhatIfDefault;
        
        public ClusterNodeConfiguration()
        {
            #region set cluster node defaults

            InMemDb = Opts.InMemDbDefault;
            Defines = Opts.DefinesDefault;
            CachedChunks = Opts.CachedChunksDefault;
            ChunksCacheSize = Opts.ChunksCacheSizeDefault;
            InternalIpAddress = Opts.InternalIpDefault;
            InternalTcpPort = Opts.InternalTcpPortDefault;
            InternalSecureTcpPort = Opts.InternalSecureTcpPortDefault;
            InternalHttpPort = Opts.InternalHttpPortDefault;
            ExternalIpAddress = Opts.ExternalIpDefault;
            ExternalTcpPort = Opts.ExternalTcpPortDefault;
            ExternalSecureTcpPort = Opts.ExternalSecureTcpPortDefault;
            ExternalHttpPort = Opts.ExternalHttpPortDefault;
            InternalHttpPrefixes = Opts.IntHttpPrefixesDefault;
            ExternalHttpPrefixes= Opts.ExtHttpPrefixesDefault;
            ClusterSize = Opts.ClusterSizeDefault;
            CertificateStoreLocation = Opts.CertificateStoreLocationDefault;
            CertificateStoreName = Opts.CertificateStoreNameDefault;
            CertificateSubjectName = Opts.CertificateSubjectNameDescr;
            CertificateThumbprint = Opts.CertificateThumbprintDefault;
            CertificateFile = Opts.CertificateFileDefault;
            CertificatePassword = Opts.CertificatePasswordDefault;
            PrepareCount = Opts.PrepareCountDefault;
            CommitCount = Opts.CommitCountDefault;
            UseInternalSsl = Opts.UseInternalSslDefault;
            SslTargetHost = Opts.SslTargetHostDefault;
            AuthenticationType = Opts.AuthenticationTypeDefault;
            AuthenticationConfigFile = Opts.AuthenticationConfigFileDefault;
            EnableTrustedAuth = Opts.EnableTrustedAuthDefault;
            WorkerThreads = Opts.WorkerThreadsDefault;
            DiscoverViaDns = Opts.DiscoverViaDnsDefault;
            GossipSeedsEndPoints = Opts.GossipSeedDefault;
            ClusterDns = Opts.ClusterDnsDefault;
            MinFlushDelayMs = Opts.MinFlushDelayMsDefault;
            PrepareTimeoutMs = Opts.PrepareTimeoutMsDefault;
            CommitTimeoutMs = Opts.CommitTimeoutMsDefault;
            SslValidateServer = Opts.SslValidateServerDefault;
            StatsPeriodSec = Opts.StatsPeriodDefault;
            NodePriority = Opts.NodePriorityDefault;
            DisableScavengeMerging = Opts.DisableScavengeMergeDefault;
            ClusterGossipPort = Opts.ClusterGossipPortDefault;
            SkipDbVerify = Opts.SkipDbVerifyDefault;
            RunProjections = Opts.RunProjectionsDefault;
            ProjectionThreads = Opts.ProjectionThreadsDefault;
            AdminOnExt = Opts.AdminOnExtDefault;
            StatsOnExt = Opts.StatsOnExtDefault;
            GossipOnExt = Opts.GossipOnExtDefault;
            ScavengeHistoryMaxAge = Opts.ScavengeHistoryMaxAgeDefault;
            AddInterfacePrefixes = Opts.AddInterfacePrefixesDefault;
            StartStandardProjections = Opts.StartStandardProjectionsDefault;

            DisableHTTPCaching = Opts.DisableHttpCachingDefault;
            LogHttpRequests = Opts.LogHttpRequestsDefault;
            EnableHistograms = Opts.HistogramEnabledDefault;
            UnsafeIgnoreHardDelete = Opts.UnsafeIgnoreHardDeleteDefault;
            UnsafeDisableFlushToDisk = Opts.UnsafeDisableFlushToDiskDefault;
            BetterOrdering = Opts.BetterOrderingDefault;
            IndexCacheDepth = Opts.IndexCacheDepthDefault;

            GossipTimeoutMs = Opts.GossipTimeoutMsDefault;
            GossipIntervalMs = Opts.GossipIntervalMsDefault;
            GossipAllowedDifferenceMs = Opts.GossipAllowedDifferenceMsDefault;
            HashCollisionReadLimit = Opts.HashCollisionReadLimitDefault;
            MaxMemTableSize = Opts.MaxMemtableSizeDefault;

            InternalTcpHeartbeatTimeout = Opts.IntTcpHeartbeatTimeoutDefault;
            InternalTcpHeartbeatInterval = Opts.IntTcpHeartbeatIntervalDefault;
            ExternalTcpHeartbeatTimeout = Opts.ExtTcpHeartbeatTimeoutDefault;
            ExternalTcpHeartbeatInterval = Opts.ExtTcpHeartbeatIntervalDefault;
            ReaderThreadsCount = Opts.ReaderThreadsCountDefault;

            #endregion
        }

        public int ReaderThreadsCount { get; set; }

        public int InternalTcpHeartbeatTimeout { get; set; }
        public int InternalTcpHeartbeatInterval { get; set; }
        public int ExternalTcpHeartbeatTimeout { get; set; }
        public int ExternalTcpHeartbeatInterval { get; set; }

        public int MaxMemTableSize { get; set; }
        public int HashCollisionReadLimit { get; set; }
        public int GossipIntervalMs { get; set; }
        public int GossipAllowedDifferenceMs { get; set; }
        public int GossipTimeoutMs { get; set; }

        public int IndexCacheDepth { get; set; }

        public bool DisableHTTPCaching { get; set; }
        public bool LogHttpRequests { get; set; }
        public bool EnableHistograms { get; set; }
        public bool UnsafeIgnoreHardDelete { get; set; }
        public bool UnsafeDisableFlushToDisk { get; set; }
        public bool BetterOrdering { get; set; }

        public bool InMemDb { get; set; }

        public bool Help
        {
            get { return m_Help; }
        }

        public bool Version
        {
            get { return m_Version; }
        }

        public string Config
        {
            get { return m_Config; }
        }

        public string Log
        {
            get { return m_Log; }
        }

        public string[] Defines { get; set; }

        public bool Force
        {
            get { return m_Force; }
        }

        public bool WhatIf
        {
            get { return m_WhatIf; }
        }

        public int CachedChunks { get; set; }
        public long ChunksCacheSize { get; set; }

        [JsonIgnore]
        public IPAddress InternalIpAddress { get; set; }

        public string InternalIp
        {
            get { return InternalIpAddress.ToString(); }
            set { InternalIpAddress = IPAddress.Parse(value); }
        }

        public int InternalTcpPort { get; set; }
        public int InternalSecureTcpPort { get; set; }
        public int InternalHttpPort { get; set; }

        [JsonIgnore]
        public IPAddress ExternalIpAddress { get; set; }

        public string ExternalIp
        {
            get { return ExternalIpAddress.ToString(); }
            set { ExternalIpAddress = IPAddress.Parse(value); }
        }

        public int ExternalTcpPort { get; set; }
        public int ExternalSecureTcpPort { get; set; }
        public int ExternalHttpPort { get; set; }

        public string[] InternalHttpPrefixes { get; set; }
        public string[] ExternalHttpPrefixes { get; set; }

        public int ClusterSize { get; set; }

        public string CertificateStoreLocation { get; set; }
        public string CertificateStoreName { get; set; }
        public string CertificateSubjectName { get; set; }
        public string CertificateThumbprint { get; set; }
        public string CertificateFile { get; set; }
        public string CertificatePassword { get; set; }

        public int PrepareCount { get; set; }
        public int CommitCount { get; set; }

        public bool UseInternalSsl { get; set; }
        public string SslTargetHost { get; set; }

        public string AuthenticationType { get; set; }
        public string AuthenticationConfigFile { get; set; }

        public bool EnableTrustedAuth { get; set; }

        public int WorkerThreads { get; set; }
        public bool DiscoverViaDns { get; set; }
        [JsonIgnore]
        public IPEndPoint[] GossipSeedsEndPoints { get; set; }

        public string[] GossipSeeds{
            get { return GossipSeedsEndPoints.Select(ep => ep.ToString()).ToArray(); }
            set { GossipSeedsEndPoints = value.Select(parseIpEndPoint).ToArray(); }
        }
        public string ClusterDns { get; set; }

        public double MinFlushDelayMs { get; set; }
        public double PrepareTimeoutMs { get; set; }
        public double CommitTimeoutMs { get; set; }

        public bool SslValidateServer { get; set; }
        public double StatsPeriodSec { get; set; }
        public int NodePriority { get; set; }

        public bool AdminOnExt { get; set; }
        public bool StatsOnExt { get; set; }
        public bool GossipOnExt { get; set; }

        public bool DisableScavengeMerging { get; set; }
        public int ScavengeHistoryMaxAge { get; set; }
        public int ClusterGossipPort { get; set; }

        public bool SkipDbVerify { get; set; }
        public ProjectionType RunProjections { get; set; }

        public int ProjectionThreads { get; set; }

        [JsonIgnore]
        public IPAddress InternalIpAddressAdvertiseAs { get; set; }
        [JsonIgnore]
        public IPAddress ExternalIpAddressAdvertiseAs { get; set; }

        public bool AddInterfacePrefixes { get; set; }
        public int InternalTcpPortAdvertiseAs { get; set; }
        public int InternalSecureTcpPortAdvertiseAs { get; set; }
        public int ExternalTcpPortAdvertiseAs { get; set; }
        public int InternalHttpPortAdvertiseAs { get; set; }
        public int ExternalSecureTcpPortAdvertiseAs { get; set; }
        public int ExternalHttpPortAdvertiseAs { get; set; }
        public bool StartStandardProjections { get; set; }

        private static IPEndPoint parseIpEndPoint(string endPoint)
        {
            string[] ep = endPoint.Split(':');
            if (ep.Length < 2) throw new FormatException("Invalid endpoint format");
            IPAddress ip;
            if (ep.Length > 2)
            {
                if (!IPAddress.TryParse(string.Join(":", ep, 0, ep.Length - 1), out ip))
                {
                    throw new FormatException("Invalid ip-adress");
                }
            }
            else
            {
                if (!IPAddress.TryParse(ep[0], out ip))
                {
                    throw new FormatException("Invalid ip-adress");
                }
            }
            int port;
            if (!int.TryParse(ep[ep.Length - 1], NumberStyles.None, NumberFormatInfo.CurrentInfo, out port))
            {
                throw new FormatException("Invalid port");
            }
            return new IPEndPoint(ip, port);
        }
    }
}