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

using System.Threading.Tasks;
using Hazelcast.Clustering;
using Hazelcast.Core;
using Hazelcast.DistributedObjects;
using Hazelcast.Serialization;
using Microsoft.Extensions.Logging;

namespace Hazelcast.CP
{
    internal abstract class CPDistributedObjectBase : DistributedObjectBase, ICPDistributedObject
    {
        protected readonly string ObjectName;

        protected CPDistributedObjectBase(RaftGroupId raftGroupId, string objectName, string serviceName, string name,
            DistributedObjectFactory factory, Cluster cluster, ISerializationService serializationService,
            ILoggerFactory loggerFactory) : base(serviceName, name, factory, cluster,
            serializationService, loggerFactory)
        {
            RaftGroupId = raftGroupId;
            ObjectName = objectName;
        }

        protected RaftGroupId RaftGroupId { get; }
        
        public ICPGroupId GroupId => RaftGroupId;

        public override async ValueTask DestroyAsync() => await Factory.DestroyAsync(RaftGroupId, ServiceName, ObjectName).CAF();

        public override ValueTask DisposeAsync() => default;
    }
}