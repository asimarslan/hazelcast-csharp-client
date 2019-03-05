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
using Hazelcast.Client.Protocol.Util;
using Hazelcast.Config;
using Hazelcast.Logging;

namespace Hazelcast.Examples.Map
{
    public class MapGetAsync
    {
        public static void Run(string[] args)
        {
            printThreadCounts();
            Environment.SetEnvironmentVariable("hazelcast.logging.level", "info");
            Environment.SetEnvironmentVariable("hazelcast.logging.type", "console");
//            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.timeout", null);
//            Environment.SetEnvironmentVariable("hazelcast.client.heartbeat.interval", null);

            var config = new ClientConfig();
            config.GetNetworkConfig().AddAddress("127.0.0.1:5701");
            config.GetNetworkConfig().SetRedoOperation(true);
            config.GetNetworkConfig().SetConnectionAttemptLimit(1000);
            config.GetNetworkConfig().SetConnectionTimeout(500000); //this is the fix

            const int TaskCount = 100;
            const int ItemCount = 1000;
            const int ValueSize = 1000;
            var mapName = "test-" + Guid.NewGuid();

            var dict = new Dictionary<string, byte[]>();
            for (var i = 0; i < ItemCount; i++)
            {
                dict.Add("key-" + i, new byte[ValueSize]);
            }
            var client = HazelcastClient.NewHazelcastClient(config);
            var map = client.GetMap<string, byte[]>(mapName);
            Console.WriteLine("Putall---------START");
            map.PutAll(dict);
            Console.WriteLine("Putall---------END");
            
            
            var results = new List<Task<byte[]>>();
            for (var i = 0; i < TaskCount; i++)
            {
                var task = map.GetAsync("key-" + i);
                results.Add(task);
            }
            Task.WaitAll(results.ToArray());

//                ThreadPool.QueueUserWorkItem((state) =>
//                {
//                    printThreadCounts();
//                    var v = map.Get("key-" + i);
//                    Console.WriteLine("Result {0}", v);
//                    ss.Signal();
//                });
//                var task = Task.Factory.StartNew(() =>
//                {
//                    var v = map.GetAsync("key-" + Interlocked.Increment(ref k));
////                    printThreadCounts();
//                    Console.WriteLine("Result {0}", v.Result);
//                    ss.Signal();
//                }, TaskCreationOptions.LongRunning);
//                
//            ss.Wait();

            foreach (var tm in ClientMessageBuilder.times)
            {
                Console.WriteLine(tm);
            }
            

//            Task.WaitAll(tasks.ToArray());
            client.Shutdown();
        }

        private static void printThreadCounts()
        {
            int compThreadCount;
            int workerThread;
            ThreadPool.GetAvailableThreads(out workerThread, out compThreadCount);

            Console.WriteLine("TP WT: {0} CT:{1}", workerThread, compThreadCount);
        }
    }
}