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

using Hazelcast.Client.Spi;
using System;
using System.Collections.Generic;
using Hazelcast.Client;
using Hazelcast.Client.Protocol;
using Hazelcast.Client.Protocol.Codec;
using Hazelcast.Client.Spi;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.IO.Serialization;
using Hazelcast.Logging;
using Hazelcast.Util;

namespace Hazelcast.NearCache
{
    public class NearCacheListener : IListenerRegistrationAware
    {
        private static readonly ILogger Logger = Logging.Logger.GetLogger(typeof(NearCacheListener));

        private void HandleIMapBatchInvalidationEvent_v1_0(IList<IData> keys)
        {
            Logger.Severe("Unexpected event from server");
        }

        private void HandleIMapBatchInvalidationEvent_v1_4(IList<IData> keys, IList<string> sourceuuids,
            IList<Guid> partitionuuids, IList<long> sequences)
        {
            _repairingHandler.Handle(keys, sourceuuids, partitionuuids, sequences);
        }

        private void HandleIMapInvalidationEvent_v1_0(IData key)
        {
            Logger.Severe("Unexpected event from server");
        }

        private void HandleIMapInvalidationEvent_v1_4(IData key, string sourceUuid, Guid partitionUuid, long sequence)
        {
            _repairingHandler.Handle(key, sourceUuid, partitionUuid, sequence);
        }

        public void HandleListenerEvent(IClientMessage eventMessage)
        {
            _distributedEventHandler(eventMessage);

            if (_supportsRepairableNearCache)
            {
                MapAddNearCacheInvalidationListenerCodec.EventHandler.HandleEvent(eventMessage, HandleIMapInvalidationEvent_v1_0,
                    HandleIMapInvalidationEvent_v1_4, HandleIMapBatchInvalidationEvent_v1_0,
                    HandleIMapBatchInvalidationEvent_v1_4);
            }
            else
            {
                MapAddNearCacheEntryListenerCodec.EventHandler.HandleEvent(eventMessage, HandleIMapInvalidationEvent_v1_0,
                    HandleIMapInvalidationEvent_v1_4, HandleIMapBatchInvalidationEvent_v1_0,
                    HandleIMapBatchInvalidationEvent_v1_4);
            }
        }

        public void BeforeListenerRegister()
        {
            _supportsRepairableNearCache = SupportsRepairableNearCache();

            if (_supportsRepairableNearCache)
            {
                _distributedEventHandler = msg => MapAddNearCacheInvalidationListenerCodec.EventHandler.HandleEvent(msg,
                    HandleIMapInvalidationEvent_v1_0, HandleIMapInvalidationEvent_v1_4, HandleIMapBatchInvalidationEvent_v1_0,
                    HandleIMapBatchInvalidationEvent_v1_4);

                RepairingTask repairingTask = GetContext().getRepairingTask(getServiceName());
                _repairingHandler = repairingTask.registerAndGetHandler(_name, nearCache);
            }
            else
            {
                _distributedEventHandler = msg => MapAddNearCacheEntryListenerCodec.EventHandler.HandleEvent(msg,
                    HandleIMapInvalidationEvent_v1_0, HandleIMapInvalidationEvent_v1_4, HandleIMapBatchInvalidationEvent_v1_0,
                    HandleIMapBatchInvalidationEvent_v1_4);

                Clear();
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingTask.DeregisterHandler(_name);
                Logger.Warning(format("Near Cache for '%s' map is started in legacy mode", _name));
            }
        }

        public void OnListenerRegister()
        {
            if (!_supportsRepairableNearCache)
            {
                Clear();
            }
        }
    }
}