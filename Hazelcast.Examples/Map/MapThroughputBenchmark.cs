// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
using System.Threading;
using Hazelcast.Client;
using Hazelcast.Config;

namespace Hazelcast.Examples.Map
{
    public class MapThroughputBenchmark
    {
        public static void Run(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine(
                    "SimpleThroughput durationSeconds threadCount valueSizeInBytes numberOfKeys address");
                return;
            }
            int durationSeconds = int.Parse(args[0]);
            Console.WriteLine("durationSeconds = " + durationSeconds);

            int threadCount = int.Parse(args[1]);
            Console.WriteLine("threadCount = " + threadCount);

            int valueSizeInBytes = int.Parse(args[2]);
            Console.WriteLine("valueSizeInBytes = " + valueSizeInBytes);

            int numberOfKeys = int.Parse(args[3]);
            Console.WriteLine("numberOfKeys = " + numberOfKeys);

            string address = args[4];
            Console.WriteLine("address = " + address);

            Environment.SetEnvironmentVariable("hazelcast.logging.level", "info");
            Environment.SetEnvironmentVariable("hazelcast.logging.type", "console");

            var config = new ClientConfig();
            config.GetNetworkConfig().AddAddress(address);
            var client = HazelcastClient.NewHazelcastClient(config);

            var map = client.GetMap<int?, byte[]>("test");
            var value = new byte[valueSizeInBytes];
            
            //fill map
            for (int i = 0; i < numberOfKeys; i++)
            {
                map.Put(i, value);
            }
            var opsPerMs = new double[threadCount];
            var threads = new Thread[threadCount];
            for (var i = 0; i < threadCount; i++) {
                var threadId = i;
                threads[i] = new Thread(() =>
                {
                    var random = new Random();
                    double count = 0;
                    int key = random.Next(numberOfKeys);

                    var begin =  DateTime.UtcNow;
                    var delta = TimeSpan.FromSeconds(durationSeconds);
                    while (true) {
                        if (count % 1000 == 0 && DateTime.UtcNow - begin > delta ) {
                            break;
                        }
                        map.Get(key);
                        count++;
                    }
                    var timePassedInMillis = (DateTime.UtcNow - begin).TotalMilliseconds;
                    opsPerMs[threadId] = count / timePassedInMillis;
                });
                threads[i].Start();
            }
            
            for (var i = 0; i < threadCount; i++) {
                try {
                    threads[i].Join();
                } catch (ThreadInterruptedException e) {
                    Console.WriteLine(e);
                }
            }
            
            double totalOpsPerMs = 0;
            for (int i = 0; i < threadCount; i++) {
                totalOpsPerMs += opsPerMs[i];
            }

            Console.WriteLine("ops/ms      = " + totalOpsPerMs);
            client.Shutdown();
        }
    }
}