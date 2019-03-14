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
using System.Collections.Generic;
using Hazelcast.Client;
using Hazelcast.Client.Protocol;
using Hazelcast.Client.Protocol.Codec;
using Hazelcast.Client.Spi;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.IO.Serialization;
using Hazelcast.Util;

namespace Hazelcast.NearCache
{
    internal class NearCache : BaseNearCache, IListenerRegistrationAware
    {
        private bool _supportsRepairableNearCache;
        private RepairingHandler _repairingHandler;

        private DistributedEventHandler _distributedEventHandler;

        public NearCache(string name, HazelcastClient client, NearCacheConfig nearCacheConfig) : base(name, client,
            nearCacheConfig)
        {
        }


        public override void Init()
        {
            if (InvalidateOnChange)
            {
                RegisterInvalidateListener();
            }
        }

        protected override NearCacheRecord CreateRecord(IData key, object value)
        {
            var record = base.CreateRecord(key, value);
            InitInvalidationMetadata(record);
            return record;
        }

        protected override bool IsStaleRead(IData key, NearCacheRecord record)
        {
            if (_repairingHandler == null)
            {
                return false;
            }
            var latestMetaData = _repairingHandler.GetMetaDataContainer(record.PartitionId);
            return record.Guid != latestMetaData.Guid || record.Sequence < latestMetaData.StaleSequence;
        }

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

        private void InitInvalidationMetadata(NearCacheRecord newRecord)
        {
            if (_repairingHandler == null)
            {
                return;
            }
            _repairingHandler.InitInvalidationMetadata(newRecord);
        }


        private void RegisterInvalidateListener()
        {
            try
            {
                RegistrationId = _clientListenerService
                    .RegisterListener(EncodeInvalidationListener, DecodeRegisterResponse,
                    id => MapRemoveEntryListenerCodec.EncodeRequest(Name, id), HandleListenerEvent);
            }
            catch (Exception e)
            {
                Logger.Severe("-----------------\n Near Cache is not initialized!!! \n-----------------", e);
            }
        }

        private string DecodeRegisterResponse(IClientMessage message)
        {
            var localUuid = Client.GetClientClusterService().GetLocalClient().GetUuid();
            _repairingHandler = new RepairingHandler(localUuid, this, Client.GetClientPartitionService());
            return MapAddNearCacheInvalidationListenerCodec.DecodeResponse(message).response;
        }
        
        private IClientMessage EncodeInvalidationListener()
        {
            return SupportsRepairableNearCache()
                ? MapAddNearCacheInvalidationListenerCodec.EncodeRequest(Name, (int) EntryEventType.Invalidation, false)
                : MapAddNearCacheEntryListenerCodec.EncodeRequest(Name, (int) EntryEventType.Invalidation, false);
        }
        private bool SupportsRepairableNearCache()
        {
            var serverVersion = ((ClientClusterService) Client.GetClientClusterService()).ServerVersion;
            return serverVersion >= VersionUtil.Version38;
        }

        private void HandleListenerEvent(IClientMessage eventMessage)
        {
            _distributedEventHandler(eventMessage);
        }

        void IListenerRegistrationAware.BeforeListenerRegister()
        {
            _supportsRepairableNearCache = SupportsRepairableNearCache();
            
            if (_supportsRepairableNearCache)
            {
                _distributedEventHandler = msg=>MapAddNearCacheInvalidationListenerCodec.EventHandler.HandleEvent(msg, 
                    HandleIMapInvalidationEvent_v1_0, HandleIMapInvalidationEvent_v1_4, 
                    HandleIMapBatchInvalidationEvent_v1_0, HandleIMapBatchInvalidationEvent_v1_4);
                
//                RepairingTask repairingTask = Client.GetRepairingTask(getServiceName());
                _repairingHandler = repairingTask.registerAndGetHandler(_name, nearCache);
                _repairingHandler = Client.registerAndGetHandler(_name, nearCache);
            }
            else 
            {
                _distributedEventHandler = msg=>MapAddNearCacheEntryListenerCodec.EventHandler.HandleEvent(msg,
                    HandleIMapInvalidationEvent_v1_0, HandleIMapInvalidationEvent_v1_4,
                    HandleIMapBatchInvalidationEvent_v1_0, HandleIMapBatchInvalidationEvent_v1_4);

                Clear();
                RepairingTask repairingTask = getContext().GetRepairingTask(getServiceName());
                repairingTask.DeregisterHandler(_name);
                Logger.Warning(format("Near Cache for '%s' map is started in legacy mode", _name));
            }

        }

        void IListenerRegistrationAware.OnListenerRegister()
        {
            if (!_supportsRepairableNearCache) {
                Clear();
            }
        }
    }
}