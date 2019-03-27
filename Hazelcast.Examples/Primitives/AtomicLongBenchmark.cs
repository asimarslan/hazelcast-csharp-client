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
using System.Threading;
using Hazelcast.Client;
using Hazelcast.Config;
using Hazelcast.Core;

namespace Hazelcast.Examples.Primitives
{
    public class AtomicLongBenchmark
    {
        private const int MaxThreadCount = 32;
        private const int MaxSize = 100000;

        private static IHazelcastInstance client;

        public static void Run(string[] args)
        {
//            ThreadPool.SetMinThreads(10000, 10000);
            Environment.SetEnvironmentVariable("hazelcast.logging.level", "info");
            Environment.SetEnvironmentVariable("hazelcast.logging.type", "console");

            var clientConfig = new ClientConfig();
//            clientConfig.GetNetworkConfig().AddAddress("192.168.1.200:5701");
            clientConfig.GetNetworkConfig().AddAddress("127.0.0.1:5701");
//            clientConfig.GetNetworkConfig().AddAddress("10.216.1.24:5701");
            clientConfig.GetNetworkConfig().SetConnectionAttemptLimit(1000);
            client = HazelcastClient.NewHazelcastClient(clientConfig);

            Console.WriteLine("Client Ready to go...");
            Console.WriteLine("Warm up: " + BenchSingle(MaxSize / 100));
//            Console.WriteLine("Warm up: " + BenchSingle(MaxSize / 10));
            Console.WriteLine("Warm up: " + Bench(1, MaxSize / 10));
            Console.WriteLine("Warm up: " + Bench(100, MaxSize));
//            return;

            Console.WriteLine("\nRESULTS...\n");
            for (var threadCount = 1; threadCount < MaxThreadCount; threadCount+=1)
            {
                GC.Collect(9, GCCollectionMode.Forced, true, true);
                var benchResult = Bench(threadCount, MaxSize);
                Console.WriteLine("{0}: {1} ops/sec", threadCount, benchResult);
            }

            client.Shutdown();
//            Console.ReadKey();
        }

        private static double Bench(int threadCount, int maxCount)
        {
            var mx = maxCount / threadCount;
            CountdownEvent cde = new CountdownEvent(threadCount);
            var atomicLong = client.GetAtomicLong("default");
            var threads = new List<Thread>();
            var sw = new Stopwatch();
            sw.Start();

            for (var i = 0; i < threadCount; i++)
            {
                var t = new Thread(() =>
                {
                    for (int j = 0; j < mx; j++)
                    {
                        atomicLong.IncrementAndGet();
                    }
                    cde.Signal();
                });
                threads.Add(t);
                t.Start();
            }
            cde.Wait();
            sw.Stop();
            return 1000 * mx * threadCount / sw.ElapsedMilliseconds;
        }

        private static double BenchSingle(int maxCount)
        {
            var atomicLong = client.GetAtomicLong("default");
            var sw = new Stopwatch();
            sw.Start();

            for (int j = 0; j < maxCount; j++)
            {
                atomicLong.IncrementAndGet();
            }
            sw.Stop();
            return 1000 * maxCount / sw.ElapsedMilliseconds;
        }
    }
}