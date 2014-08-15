using System;
using System.Globalization;
using System.Linq;
using System.Net;
using EventStore.Common.Options;
using EventStore.Core.Util;
using Newtonsoft.Json;

namespace Inceptum.Applications.EventStoreNode.Cluster
{
    public class ClusterNodeConfiguration
    {
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
            HttpPrefixes = Opts.HttpPrefixesDefault;
            ClusterSize = Opts.ClusterSizeDefault;
            
            CertificateStore = Opts.CertificateStoreLocationDefault;
            CertificateName = Opts.CertificateStoreNameDefault;
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

            #endregion
        }

        public bool InMemDb { get; set; }

        public string[] Defines { get; set; }

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

        public string[] HttpPrefixes { get; set; }

        public int ClusterSize { get; set; }

        public string CertificateStore { get; set; }
        public string CertificateName { get; set; }
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

        public int ClusterGossipPort { get; set; }

        public bool SkipDbVerify { get; set; }
        public ProjectionType RunProjections { get; set; }

        public int ProjectionThreads { get; set; }

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