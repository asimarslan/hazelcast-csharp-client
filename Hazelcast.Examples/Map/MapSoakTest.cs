// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hazelcast.Client;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.Examples.Models;
using Hazelcast.Logging;

namespace Hazelcast.Examples.Map
{
    public class MapSoakTest
    {
        private readonly ILogger _logger;

        private const int ThreadCount = 32;
        private const int EntryCount = 10000;
        private static readonly ConcurrentDictionary<int, long> _stats = new ConcurrentDictionary<int, long>(ThreadCount, ThreadCount);

        private static readonly DateTime _startTime = DateTime.Now;
        
        internal static void Run(string[] args)
        {
            var test = new MapSoakTest(args);

            test.ExecuteTests();
        }

        private readonly IHazelcastInstance client;

        private MapSoakTest(params string[] remoteServerAddresses)
        {
            Environment.SetEnvironmentVariable("hazelcast.logging.level", "finest");
            Environment.SetEnvironmentVariable("hazelcast.logging.type", "console");
            _logger = Logger.GetLogger(GetType().Name);

            TaskScheduler.UnobservedTaskException += UnobservedTaskException;
            var clientConfig = new ClientConfig();
            foreach (var serverAddress in remoteServerAddresses)
            {
                clientConfig.GetNetworkConfig().AddAddress(serverAddress);
            }
            clientConfig.AddListenerConfig(new ListenerConfig(new EntryListener()));
            client = HazelcastClient.NewHazelcastClient(clientConfig);
            
            for (int i = 0; i < ThreadCount; i++)
            {
                _stats.TryAdd(i, 0);
            }
        }

        private void UnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
        {
            _logger.Warning("UnobservedTaskException Error sender:" + sender);
            _logger.Warning("UnobservedTaskException Error.", e.Exception);
        }

        private void ExecuteTests()
        {
            Console.WriteLine("START tests!!!");
            //register hook for Ctrl-c on console.
            var source = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, args) =>
            {
                args.Cancel = true;
                source.Cancel();
            };
            var token = source.Token;
            var tasks = new List<Task>();
            for (var i = 0; i < ThreadCount; i++)
            {
                var t = new Task(id => TestMap(client, token, (int) id), i, TaskCreationOptions.LongRunning);
                tasks.Add(t);
                t.Start();
            }

            try
            {
                Task.WaitAll(tasks.ToArray(), token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("cancel all running tests!!!");
            }

            Console.WriteLine("TEST TOTAL OPERATION COUNT:");
            Console.WriteLine("---------------------------");
            for (int i = 0; i < ThreadCount; i++)
            {
                Console.WriteLine("Thread id:{0} total operation count: {1}", i, _stats[i]);
            }
            Console.WriteLine("END tests!!!");
        }

        private static void TestMap(IHazelcastInstance hz, CancellationToken ct, int id)
        {
            try
            {
                var random = new Random();
                var map = hz.GetMap<string, string>("default");
                while (!ct.IsCancellationRequested && (DateTime.Now - _startTime).TotalHours < 48)
                {
                    try
                    {
                        var key = random.Next(0, EntryCount).ToString();
                        var operation = random.Next(0, 100);
                        if (operation < 30)
                        {
                            map.Get(key);
                        }
                        else if (operation < 60)
                        {
                            map.Put(key, random.Next().ToString());
                        }
                        else if (operation < 80)
                        {
                            map.Values(Predicates.IsBetween("this", 0, 10));
                        }
                        else
                        {
                            map.ExecuteOnKey(key, new UpdateEntryProcessor(key));
                        }
                        _stats[id] += 1;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }
    }

    internal class EntryListener : IEntryListener<string, string>
    {
        public void EntryAdded(EntryEvent<string, string> entryEvent)
        {
            entryEvent.GetKey();
            entryEvent.GetValue();
            entryEvent.GetOldValue();
        }

        public void EntryUpdated(EntryEvent<string, string> entryEvent)
        {
            entryEvent.GetKey();
            entryEvent.GetValue();
            entryEvent.GetOldValue();
        }

        public void EntryRemoved(EntryEvent<string, string> entryEvent)
        {
            entryEvent.GetKey();
            entryEvent.GetValue();
            entryEvent.GetOldValue();
        }

        public void EntryEvicted(EntryEvent<string, string> entryEvent)
        {
            entryEvent.GetKey();
            entryEvent.GetValue();
            entryEvent.GetOldValue();
        }

        public void MapCleared(MapEvent mapEvent)
        {
            mapEvent.GetNumberOfEntriesAffected();
        }

        public void MapEvicted(MapEvent @mapEvent)
        {
            mapEvent.GetNumberOfEntriesAffected();
        }
    }
}