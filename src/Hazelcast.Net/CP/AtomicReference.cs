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
    internal class AtomicReference<T> : CPDistributedObjectBase, IAtomicReference<T>
    {
        public AtomicReference(RaftGroupId raftGroupId, string objectName, string name,
            DistributedObjectFactory factory, Cluster cluster, ISerializationService serializationService,
            ILoggerFactory loggerFactory) : base(raftGroupId, objectName, ServiceNames.AtomicReference, name, factory, cluster,
            serializationService, loggerFactory)
        {
        }

        public async Task<T> GetAsync()
        {
            var request = AtomicRefGetCodec.EncodeRequest(RaftGroupId, ObjectName);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return ToObject<T>(AtomicRefGetCodec.DecodeResponse(response).Response);
        }

        public async Task<bool> CompareExchangeAsync(T value, T comparand)
        {
            var request = AtomicRefCompareAndSetCodec.EncodeRequest(RaftGroupId, ObjectName,
                ToSafeData(comparand), ToSafeData(value));
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicRefCompareAndSetCodec.DecodeResponse(response).Response;
        }

        public async Task<T> GetAndExchangeAsync(T value)
        {
            var request = AtomicRefSetCodec.EncodeRequest(RaftGroupId, ObjectName, ToSafeData(value), true);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return ToObject<T>(AtomicRefSetCodec.DecodeResponse(response).Response);
        }

        public async Task ExchangeAsync(T value) => await ExchangeAsyncInternal(ToSafeData(value)).CAF();

        public async Task<bool> IsNullAsync() => await ContainsAsyncInternal(null).CAF();

        public async Task ClearAsync() => await  ExchangeAsyncInternal(null).CAF();

        public async Task<bool> ContainsAsync(T comparand) => await ContainsAsyncInternal(ToSafeData(comparand)).CAF();

        private async Task ExchangeAsyncInternal(IData valueData)
        {
            var request = AtomicRefSetCodec.EncodeRequest(RaftGroupId, ObjectName, valueData, false);
            await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
        }

        private async Task<bool> ContainsAsyncInternal(IData comparandData)
        {
            var request = AtomicRefContainsCodec.EncodeRequest(RaftGroupId, ObjectName, comparandData);
            var response = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return AtomicRefContainsCodec.DecodeResponse(response).Response;
        }
    }
}