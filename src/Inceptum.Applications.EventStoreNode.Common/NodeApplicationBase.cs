using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Castle.Core.Logging;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;

namespace Inceptum.Applications.EventStoreNode.Common
{
    public class NodeApplicationBase
    {
        private readonly ILoggerFactory m_LoggerFactory;

        public NodeApplicationBase(ILoggerFactory loggerFactory)
        {
            m_LoggerFactory = loggerFactory;
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

        protected static X509Certificate2 LoadCertificateFromStore(string storeName, string certName)
        {
            var store = new X509Store(storeName);
            try
            {
                store.Open(OpenFlags.OpenExistingOnly);
            }
            catch (Exception exc)
            {
                throw new Exception(string.Format("Couldn't open certificates store '{0}'.", storeName), exc);
            }
            foreach (var cert in store.Certificates)
            {
                if (cert.Subject == certName)
                    return cert;
            }
            throw new ArgumentException(string.Format("Certificate '{0}' not found in storage '{1}'.", certName, storeName));
        }
    }
}