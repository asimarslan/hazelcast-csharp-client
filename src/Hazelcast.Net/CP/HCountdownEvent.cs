// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
using System.Threading.Tasks;
using Hazelcast.Clustering;
using Hazelcast.Core;
using Hazelcast.DistributedObjects;
using Hazelcast.Protocol.Codecs;
using Hazelcast.Serialization;
using Microsoft.Extensions.Logging;

namespace Hazelcast.CP
{
    internal class HCountdownEvent : CPDistributedObjectBase, ICountdownEvent
    {
        public HCountdownEvent(RaftGroupId raftGroupId, string objectName, string name,
            DistributedObjectFactory factory, Cluster cluster, ISerializationService serializationService,
            ILoggerFactory loggerFactory) : base(raftGroupId, objectName, ServiceNames.CountDownLatch, name, factory, cluster,
            serializationService, loggerFactory)
        {
        }

        public async Task<int> CurrentCountAsync()
        {
            var request = CountDownLatchGetCountCodec.EncodeRequest(RaftGroupId, ObjectName);
            var response = await Cluster.Messaging.SendAsync(request).CAF();
            return CountDownLatchGetCountCodec.DecodeResponse(response).Response;
        }

        public async Task SignalAsync()
        {
            var round = await GetRound().CAF();
            var invocationUid = new Guid();
            for (;;)
            {
                try
                {
                    await CountDownAsync(round, invocationUid).CAF();
                    return;
                }
                catch (TimeoutException)
                {
                    // I can retry safely because my retry would be idempotent...
                }
            }
        }

        public async Task<bool> TryResetAsync(int count)
        {
            var request = CountDownLatchTrySetCountCodec.EncodeRequest(RaftGroupId, ObjectName, count);
            var response = await Cluster.Messaging.SendAsync(request).CAF();
            return CountDownLatchTrySetCountCodec.DecodeResponse(response).Response;
        }

        public async Task<bool> WaitAsync(TimeSpan timeout)
        {
            var invocationGuid = new Guid();
            var timeoutMillis = Math.Max(0, timeout.TotalMilliseconds);
            var request = CountDownLatchAwaitCodec.EncodeRequest(RaftGroupId, ObjectName, invocationGuid, (long) timeoutMillis);
            var response = await Cluster.Messaging.SendAsync(request).CAF();
            return CountDownLatchAwaitCodec.DecodeResponse(response).Response;
        }

        private async Task<int> GetRound()
        {
            var request = CountDownLatchGetRoundCodec.EncodeRequest(RaftGroupId, ObjectName);
            var response = await Cluster.Messaging.SendAsync(request).CAF();
            return CountDownLatchGetRoundCodec.DecodeResponse(response).Response;
        }

        private async Task CountDownAsync(int round, Guid invocationUid)
        {
            var request = CountDownLatchCountDownCodec.EncodeRequest(RaftGroupId, ObjectName, invocationUid, round);
            await Cluster.Messaging.SendAsync(request).CAF();
        }
    }
}