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
    internal class HSemaphore : CPDistributedObjectBase, ISemaphore
    {
        private readonly CPSessionManager _sessionManager;
        
        public HSemaphore(RaftGroupId raftGroupId, string objectName, string name,
            DistributedObjectFactory factory, Cluster cluster, ISerializationService serializationService,
            ILoggerFactory loggerFactory) : base(raftGroupId, objectName, ServiceNames.Semaphore, name, factory, cluster,
            serializationService, loggerFactory)
        {
        }

        /// <inheritdoc/>
        public async Task<int> CurrentCountAsync()
        {
            var request = SemaphoreAvailablePermitsCodec.EncodeRequest(RaftGroupId, ObjectName);
            var responseMessage = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return SemaphoreAvailablePermitsCodec.DecodeResponse(responseMessage).Response;
        }

        /// <inheritdoc/>
        public async Task<bool> TryInitializeCountAsync(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
            var request = SemaphoreInitCodec.EncodeRequest(RaftGroupId, ObjectName, count);
            var responseMessage = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            return SemaphoreInitCodec.DecodeResponse(responseMessage).Response;
        }

        /// <inheritdoc/>
        public async Task AddCountAsync(int increase)
        {
            throw new NotImplementedException();

            // var request = Semaphore.EncodeRequest(RaftGroupId, ObjectName, count);
            // var responseMessage = await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
            // return SemaphoreInitCodec.DecodeResponse(responseMessage).Response;
        }

        /// <inheritdoc/>
        public async Task DecrementCountAsync(int reduction)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task<int> WaitRemainingAsync()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task ReleaseAsync(int permits = 1)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task<bool> TryWaitAsync(int permits = 1, TimeSpan timeout = default)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task WaitAsync(int permits = 1)
        {
            throw new NotImplementedException();
        }
        
        private async Task ChangePermitsAsync(int delta)
        {
            var clusterWideThreadId = _sessionManager.GetOrCreateUniqueThreadIdAsync(RaftGroupId);
            var invocationUid = Guid.NewGuid();

            var request = SemaphoreChangeCodec.EncodeRequest(RaftGroupId, ObjectName, CPSessionManager.NoSessionId, 
                clusterWideThreadId, invocationUid, delta);
            await Cluster.Messaging.SendAsync(request, CancellationToken.None).CAF();
        }

    }
}