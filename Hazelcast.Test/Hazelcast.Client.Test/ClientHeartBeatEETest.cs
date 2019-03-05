// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.Remote;
using Hazelcast.Test;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace Hazelcast.Client.Test
{
    [TestFixture]
    public class ClientHeartBeatEETest : HazelcastTestSupport
    {
        private RemoteController.Client _remoteController;
        private Cluster _cluster;

        [SetUp]
        public void Setup()
        {
            _remoteController = CreateRemoteController();
            _cluster = CreateCluster(_remoteController, GetServerConfig());
            _remoteController.startMember(_cluster.Id);
        }

        [TearDown]
        public void TearDown()
        {
            HazelcastClient.ShutdownAll();
            StopCluster(_remoteController, _cluster);
            StopRemoteController(_remoteController);
        }

        protected override void ConfigureGroup(ClientConfig config)
        {
            config.GetGroupConfig().SetName(_cluster.Id).SetPassword(_cluster.Id);
        }

        protected override void ConfigureClient(ClientConfig config)
        {
            config.GetNetworkConfig().SetRedoOperation(true);
            config.GetNetworkConfig().SetConnectionAttemptLimit(1);
            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.timeout", "5000");
            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.interval", "1000");
            //Environment.SetEnvironmentVariable("hazelcast.client.statistics.enabled", "true");
        }

        [OneTimeTearDown]
        public void RestoreEnvironmentVariables()
        {
            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.timeout", null);
            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.interval", null);
            Environment.SetEnvironmentVariable("hazelcast.client.statistics.enabled", null);
        }

        private string GetServerConfig()
        {
            return Resources.hazelcast_hbee;
        }

        [Test]
        public void TestContinuousPutAll()
        {
            var client = CreateClient();
            var clientDisconnected = TestSupport.WaitForClientState(client, LifecycleEvent.LifecycleState.ClientDisconnected);

            var map = client.GetMap<string, string>("test-" + TestSupport.RandomString());

            var dict = new Dictionary<string, string>();
            for (var i = 0; i < 50000; i++)
            {
                dict.Add("key-" + i, "value-" + i);
            }

            
            var result = Parallel.For(0, 40, n =>
            {
                var sw = new Stopwatch();
                sw.Start();
                while (sw.ElapsedMilliseconds < 120000)
                {
                    map.PutAll(dict);
                }
            });

            while (!result.IsCompleted) Thread.Sleep(100);
            
            Assert.False(clientDisconnected.Wait(1000), "Client should not be disconnected");
        }

        [Test]
        public void TestContinuousPutAll2()
        {
            var client = CreateClient();

            var map = client.GetMap<string, string>("test-" + TestSupport.RandomString());

            var dict = new Dictionary<string, string>();
            for (var i = 0; i < 100000; i++)
            {
                dict.Add("key-" + i, "value-" + i);
            }

            var tasks= new List<Task>();
            for (var i = 0; i < 100; i++)
            {
                var task = Task.Factory.StartNew(() =>
                {
                    for (var n = 0; n < 3; n++)
                    {
                        map.PutAll(dict);
                    }
                }, TaskCreationOptions.LongRunning);                
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
        }        
    }
}