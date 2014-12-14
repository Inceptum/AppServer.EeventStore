using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using NUnit.Framework;
using Newtonsoft.Json;

namespace Inceptum.Applications.EventStoreNode.Tests
{
    [TestFixture]
    public class SimpleTests
    {
        [Test, Ignore]
        public async void ConnectsToLocalEventStoreClusterTest()
        {
            using (var connection = 
                EventStoreConnection.Create(ConnectionSettings.Create().KeepReconnecting(),
                ClusterSettings.Create()
                               .DiscoverClusterViaGossipSeeds()
                               .SetGossipTimeout(TimeSpan.FromMilliseconds(500))
                               .SetGossipSeedEndPoints(new[]
                                   {
                                       new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1113),
                                       new IPEndPoint(IPAddress.Parse("127.0.0.1"), 2113),
                                       new IPEndPoint(IPAddress.Parse("127.0.0.1"), 3113)
                                   })))
            {
                await connection.ConnectAsync();

                int i = 1000;
                while (i-- > 0)
                {
                    var data = new
                        {
                            sender = "Test",
                            message = string.Format("Test Message {0:hh:mm:ss.fff}", DateTime.Now),
                            time = string.Format("{0:HH:mm:ss}", DateTime.Now)

                        };
                    var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data, new JsonSerializerSettings()));
                    var ed = new EventData(Guid.NewGuid(), "ChatMessage", true, bytes, null);

                    var result = await connection.AppendToStreamAsync("chat-GeneralChat", -2, new EventData[] {ed});
                    Console.WriteLine(JsonConvert.SerializeObject(result, Formatting.Indented));
                    Thread.Sleep(1234);
                }

            }


        }
    }
}