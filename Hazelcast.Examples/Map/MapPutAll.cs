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
using System.Threading;
using System.Threading.Tasks;
using Hazelcast.Client;
using Hazelcast.Config;
using Hazelcast.Logging;

namespace Hazelcast.Examples.Map
{
    public class MapPutAll
    {
        public static void Run(string[] args)
        {
            Environment.SetEnvironmentVariable("hazelcast.logging.level", "info");
            Environment.SetEnvironmentVariable("hazelcast.logging.type", "console");
//            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.timeout", null);
//            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.interval", null);

            var config = new ClientConfig();
            config.GetNetworkConfig().AddAddress("127.0.0.1:5701");
            config.GetNetworkConfig().SetRedoOperation(true);
            config.GetNetworkConfig().SetConnectionAttemptLimit(1000);
            config.GetNetworkConfig().SetConnectionTimeout(5000);//this is the fix
            
            const int ClientCount  = 100;
            const int ItemCount  = 200000;
            var mapName = "test-" + Guid.NewGuid();

            var dict = new Dictionary<string, string>();
            for (var i = 0; i < ItemCount; i++)
            {
                dict.Add("key-" + i, "value-" + i);
            }
            var tasks = new List<Task>();
            for (var i = 0; i < ClientCount; i++)
            {
                var task = Task.Factory.StartNew(() =>
                {
                    var client = HazelcastClient.NewHazelcastClient(config);
                    var map = client.GetMap<string, string>(mapName);

                    map.PutAll(dict);
                    client.Shutdown();
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}