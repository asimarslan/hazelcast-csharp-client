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
using Hazelcast.Core;
using Hazelcast.CP;
using Hazelcast.Protocol.Codecs;

namespace Hazelcast.DistributedObjects
{
    // ReSharper disable once InconsistentNaming
    internal partial class DistributedObjectFactory
    {
        private const string DefaultGroupName = "default";

        public async Task<T> GetOrCreateAsync<T>(string serviceName, string name) where T : CPDistributedObjectBase
        {
            if (_disposed == 1) throw new ObjectDisposedException("DistributedObjectFactory");
            await _cluster.ThrowIfNotConnected().CAF();

            name = WithoutDefaultGroupName(name);
            var objectName = ToObjectName(name);
            var groupId = await GetGroupIdAsync(name, objectName).CAF();
            var o = serviceName switch
            {
                ServiceNames.AtomicLong => new AtomicLong(groupId, objectName, name, this, _cluster, _serializationService,
                    _loggerFactory),
                ServiceNames.AtomicReference => new AtomicReference<T>(groupId, objectName, name, this, _cluster,
                    _serializationService, _loggerFactory),
                ServiceNames.CountDownLatch => new HCountdownEvent(groupId, objectName, name, this, _cluster,
                    _serializationService, _loggerFactory),
                ServiceNames.FencedLock => CreateLock(groupId, name, objectName),
                ServiceNames.Semaphore => CreateSemaphore(groupId, name, objectName),
                _ => throw new ArgumentException($"Invalid service {serviceName}")
            };
            
            // race condition: maybe the factory has been disposed and is already disposing
            // objects and will ignore this new object even though it has been added to the
            // dictionary, so take care of it ourselves
            if (_disposed == 1)
            {
                await o.DisposeAsync().CAF();
                throw new ObjectDisposedException("DistributedObjectFactory");
            }

            return (T) o;
        }


        public async ValueTask DestroyAsync(RaftGroupId raftGroupId, string serviceName, string objectName,
            CancellationToken cancellationToken = default)
        {
            var request = CPGroupDestroyCPObjectCodec.EncodeRequest(raftGroupId, serviceName, objectName);
            await _cluster.Messaging.SendAsync(request, cancellationToken).CAF();
        }

        private DistributedObjectBase CreateSemaphore(RaftGroupId raftGroupId, string name, string objectName)
        {
            throw new NotImplementedException();
        }

        private DistributedObjectBase CreateLock(RaftGroupId raftGroupId, string name, string objectName)
        {
            throw new NotImplementedException();
        }

        private async Task<RaftGroupId> GetGroupIdAsync(string name, string objectName)
        {
            var request = CPGroupCreateCPGroupCodec.EncodeRequest(name);
            var response = await _cluster.Messaging.SendAsync(request).CAF();
            return CPGroupCreateCPGroupCodec.DecodeResponse(response).GroupId;
        }

        private static string WithoutDefaultGroupName(string name)
        {
            name = name.Trim();
            var i = name.IndexOf("@", StringComparison.Ordinal);
            if (i == -1)
            {
                return name;
            }

            if (name.IndexOf("@", i + 1) == -1)
                throw new ArgumentException("Custom group name must be specified at most once");
            var groupName = name.Substring(i + 1).Trim();
            return groupName.Equals(DefaultGroupName, StringComparison.OrdinalIgnoreCase)
                ? name.Substring(0, i)
                : name;
        }

        private static string ToObjectName(string name)
        {
            var i = name.IndexOf("@", StringComparison.Ordinal);
            if (i == -1)
            {
                return name;
            }

            if (i < (name.Length - 1)) throw new ArgumentException("Object name cannot be empty string");
            if (name.IndexOf("@", i + 1) == -1)
                throw new ArgumentException("Custom CP group name must be specified at most once");
            var objectName = name.Substring(0, i).Trim();
            if (objectName.Length > 0) throw new ArgumentException("Object name cannot be empty string");
            return objectName;
        }
    }
}