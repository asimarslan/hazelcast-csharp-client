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

using System.Threading;
using System.Threading.Tasks;
using Hazelcast.Clustering;
using Hazelcast.Core;
using Hazelcast.DistributedObjects;
using Hazelcast.Protocol.Codecs;
using Hazelcast.Serialization;
using Microsoft.Extensions.Logging;

namespace Hazelcast.CP
{
    internal class AtomicLong : CPDistributedObjectBase, IAtomicLong
    {
        public AtomicLong(RaftGroupId raftGroupId, string objectName, string name,
            DistributedObjectFactory factory, Cluster cluster, ISerializationService serializationService,
            ILoggerFactory loggerFactory) : base(raftGroupId, objectName, ServiceNames.AtomicLong, name, factory, cluster,
            serializationService, loggerFactory)
        {
        }

        public async Task<long> GetAsync()
        {
            var request = AtomicLongGetCodec.EncodeRequest(RaftGroupId, ObjectName);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicLongGetCodec.DecodeResponse(response).Response;
        }

        public async Task<long> AddAndGetAsync(long delta)
        {
            var request = AtomicLongAddAndGetCodec.EncodeRequest(RaftGroupId, ObjectName, delta);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicLongAddAndGetCodec.DecodeResponse(response).Response;
        }

        public async Task<long> GetAndAddAsync(long delta)
        {
            var request = AtomicLongGetAndAddCodec.EncodeRequest(RaftGroupId, ObjectName, delta);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicLongGetAndAddCodec.DecodeResponse(response).Response;
        }

        public Task<long> GetAndDecrementAsync()
        {
            return GetAndAddAsync(-1);
        }

        public Task<long> DecrementAndGetAsync()
        {
            return AddAndGetAsync(-1);
        }

        public Task<long> IncrementAndGetAsync()
        {
            return AddAndGetAsync(1);
        }

        public Task<long> GetAndIncrementAsync()
        {
            return GetAndAddAsync(1);
        }

        public async Task<bool> CompareExchangeAsync(long value, long comparand)
        {
            var request = AtomicLongCompareAndSetCodec.EncodeRequest(RaftGroupId, ObjectName, comparand, value);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicLongCompareAndSetCodec.DecodeResponse(response).Response;
        }

        public async Task<long> GetAndExchangeAsync(long value)
        {
            var request = AtomicLongGetAndSetCodec.EncodeRequest(RaftGroupId, ObjectName, value);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicLongGetAndSetCodec.DecodeResponse(response).Response;
        }

        public Task ExchangeAsync(long value)
        {
            return GetAndExchangeAsync(value);
        }
    }
}