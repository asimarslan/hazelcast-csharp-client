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
using Hazelcast.Client;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.Examples.Models;

namespace Hazelcast.Examples.Concurrency
{
    public class MultiThreadMap
    {
        public static int THREAD_COUNT = 32;
        public static int ENTRY_COUNT = 10 * 1000;
        public static int STATS_SECONDS = 10;
        public static int GET_PERCENTAGE = 40;
        public static int PUT_PERCENTAGE = 40;

        public static bool Cancelled;

        public static readonly LogData logData = new LogData("66",
            new Message("exceptionX", "LEVEL", "the message", "logger-X", "src-x", 77));

        public static void Run(string[] args)
        {
            Environment.SetEnvironmentVariable("hazelcast.logging.level", "Info");
            Environment.SetEnvironmentVariable("hazelcast.logging.type", "console");

            var clientConfig = new ClientConfig();
            clientConfig.GetNetworkConfig().AddAddress("127.0.0.1:5701");
            clientConfig.GetNetworkConfig().SetConnectionAttemptLimit(1000);
            clientConfig.GetSerializationConfig()
                .AddPortableFactoryClass(LogPortableFactory.FactoryId, typeof(LogPortableFactory));
            var hazelcast = HazelcastClient.NewHazelcastClient(clientConfig);

            Console.CancelKeyPress += (sender, argz) => { Cancelled = true; };

            Console.WriteLine("Client Ready to go");
            var tasks = new List<Thread>();
            var statsList = new List<Stats>();
            for (var i = 0; i < THREAD_COUNT; i++)
            {
                var stats = new Stats {Id = i};
                statsList.Add(stats);
                var t = new Thread(() => HzTask(hazelcast, stats));
                tasks.Add(t);
                t.Start();
            }

            var tm = new Thread(() => StatDisplayTask(hazelcast, statsList));
            tm.Start();

            tm.Join();

            Console.WriteLine("--THE END--");
        }

        public static void HzTask(IHazelcastInstance hz, Stats stats)
        {
            try
            {
                var random = new Random();
                var map = hz.GetMap<string, LogData>("default");
                while (true)
                {
                    try
                    {
                        var key = random.Next(0, ENTRY_COUNT);
                        var operation = random.Next(0, 100);
                        if (operation < GET_PERCENTAGE)
                        {
                            map.Get(key.ToString());
                            Interlocked.Increment(ref stats.Gets);
                        }
                        else if (operation < GET_PERCENTAGE + PUT_PERCENTAGE)
                        {
                            map.Set(key.ToString(), logData);
                            Interlocked.Increment(ref stats.Puts);
                        }
                        else
                        {
                            map.Remove(key.ToString());
                            Interlocked.Increment(ref stats.Removes);
                        }
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref stats.Exceptions);
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            Console.WriteLine("DONE...");
        }

        public static void StatDisplayTask(IHazelcastInstance hazelcast, List<Stats> stats)
        {
            while (!Cancelled)
            {
                Thread.Sleep(STATS_SECONDS * 1000);
                Console.WriteLine("------------------------------------------------------------------");
                Console.WriteLine("cluster size:" + hazelcast.GetCluster().GetMembers().Count);

                var opsum = 0l;
                var tc = 0;
                foreach (var stat in stats)
                {
                    try
                    {
                        var currentStats = stat.GetAndReset();
                        opsum += currentStats.Total;
                        if (currentStats.Total == 0)
                        {
                            tc++;
                            Console.WriteLine("Thread No Operation Error :" + currentStats);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
                if (tc == 0)
                {
                    Console.WriteLine("NO WAITING WORKER THREAD");
                }
                Console.WriteLine("------------------------------------------------------------------");

                Console.WriteLine("Operations per Second : " + opsum / STATS_SECONDS);
            }
        }
    }

    public class Stats
    {
        public int Id;
        public long Exceptions;
        public long Gets;
        public long Puts;
        public long Removes;

        public Stats GetAndReset()
        {
            var putsNow = Interlocked.Exchange(ref Puts, 0);
            var getsNow = Interlocked.Exchange(ref Gets, 0);
            var removesNow = Interlocked.Exchange(ref Removes, 0);
            var exceptionsNow = Interlocked.Exchange(ref Exceptions, 0);
            return new Stats {Id = Id, Puts = putsNow, Gets = getsNow, Removes = removesNow, Exceptions = exceptionsNow};
        }

        public override string ToString()
        {
            return string.Format("Stat[{0} - total= {1} ]", Id, Total);
        }

        public long Total
        {
            get { return Interlocked.Read(ref Gets) + Interlocked.Read(ref Puts) + Interlocked.Read(ref Removes); }
        }
    }
}