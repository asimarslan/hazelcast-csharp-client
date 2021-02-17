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
using Hazelcast.DistributedObjects;
using Hazelcast.Serialization;
using Microsoft.Extensions.Logging;

namespace Hazelcast.CP
{
    internal class FencedLock : CPDistributedObjectBase, IFencedLock
    {
        public FencedLock(RaftGroupId raftGroupId, string objectName, string name,
            DistributedObjectFactory factory, Cluster cluster, ISerializationService serializationService,
            ILoggerFactory loggerFactory) : base(raftGroupId, objectName, ServiceNames.FencedLock, name, factory, cluster,
            serializationService, loggerFactory)
        {
        }

        public Task EnterInterruptiblyAsync()
        {
            throw new NotImplementedException();
        }

        public Task EnterAsync()
        {
            throw new NotImplementedException();
        }

        public Task<long> EnterAndGetFenceAsync()
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryEnterAsync(TimeSpan timeout = default)
        {
            throw new NotImplementedException();
        }

        public Task<long> TryEnterAndGetFenceAsync(TimeSpan timeout = default)
        {
            throw new NotImplementedException();
        }

        public Task ExitAsync()
        {
            throw new NotImplementedException();
        }

        public Task<long> GetFenceAsync()
        {
            throw new NotImplementedException();
        }

        public Task<int> GetLockCountAsync()
        {
            throw new NotImplementedException();
        }

        public Task<bool> IsEnteredAsync()
        {
            throw new NotImplementedException();
        }

        public Task<bool> IsEnteredByCurrentThreadAsync()
        {
            throw new NotImplementedException();
        }
    }
}