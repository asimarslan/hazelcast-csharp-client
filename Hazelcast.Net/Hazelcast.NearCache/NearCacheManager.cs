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
using Hazelcast.Client;
using Hazelcast.Client.Spi;
using Hazelcast.Logging;
using Hazelcast.Util;

namespace Hazelcast.NearCache
{
    internal class NearCacheManager
    {
        private const string MaxToleratedMissCountProperty = "hazelcast.invalidation.max.tolerated.miss.count";
        private const int MaxToleratedMissCountDefault = 10;

        private static readonly ILogger Logger = Logging.Logger.GetLogger(typeof(NearCacheManager));

        private readonly ConcurrentDictionary<string, BaseNearCache> _caches =
            new ConcurrentDictionary<string, BaseNearCache>();

        private readonly HazelcastClient _client;
        private readonly RepairingTask _repairingTask;

        public NearCacheManager(HazelcastClient client)
        {
            _client = client;
            _repairingTask = new RepairingTask(client);
        }

        public void DestroyNearCache(string name)
        {
            BaseNearCache nearCache;
            if (_caches.TryRemove(name, out nearCache))
            {
                nearCache.Destroy();
            }
        }

        public ICollection<BaseNearCache> GetAllNearCaches()
        {
            return _caches.Values;
        }

        public BaseNearCache GetOrCreateNearCache(string mapName)
        {
            var nearCacheConfig = _client.GetClientConfig().GetNearCacheConfig(mapName);
            return nearCacheConfig == null
                ? null
                : _caches.GetOrAdd(mapName, newMapName =>
                {
                    var nearCache = new NearCache(newMapName, _client, nearCacheConfig);
//                    if (SupportsRepairableNearCache())
//                    {
//                        nearCache = new NearCache(newMapName, _client, nearCacheConfig);
//                    }
//                    else
//                    {
//                        nearCache = new NearCachePre38(newMapName, _client, nearCacheConfig);
//                    }
                    InitNearCache(nearCache);
                    return nearCache;
                });
        }

        public bool TryGetCache(string mapName, out BaseNearCache cache)
        {
            return _caches.TryGetValue(mapName, out cache);
        }
        
        public void Shutdown()
        {
            _repairingTask.Shutdown();
            DestroyAllNearCache();
        }

        internal static int GetMaxToleratedMissCount()
        {
            var maxToleratedMissCount =
                EnvironmentUtil.ReadInt(MaxToleratedMissCountProperty) ?? MaxToleratedMissCountDefault;
            return ValidationUtil.CheckNotNegative(maxToleratedMissCount,
                string.Format("max-tolerated-miss-count cannot be < 0 but found {0}", maxToleratedMissCount));
        }

        private void DestroyAllNearCache()
        {
            foreach (var entry in _caches)
            {
                DestroyNearCache(entry.Key);
            }
        }

        private void InitNearCache(BaseNearCache baseNearCache)
        {
            try
            {
                baseNearCache.Init();
                var nearCache = baseNearCache as NearCache;
                if (nearCache == null) return;

                var repairingHandler = nearCache.RepairingHandler;
                if (repairingHandler == null) return;

                _repairingTask.InitMetadata(nearCache.Name, responseParameter =>
                {
                    repairingHandler.InitGuids(responseParameter.partitionUuidList);
                    repairingHandler.InitSequences(responseParameter.namePartitionSequenceList);
                });

                _repairingTask.Start();
            }
            catch (Exception e)
            {
                Logger.Warning(e);
            }
        }

        private bool SupportsRepairableNearCache()
        {
            var serverVersion = ((ClientClusterService) _client.GetClientClusterService()).ServerVersion;
            return serverVersion >= VersionUtil.Version38;
        }
    }
}