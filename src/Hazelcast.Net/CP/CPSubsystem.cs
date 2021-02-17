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
using Hazelcast.Core;
using Hazelcast.DistributedObjects;

namespace Hazelcast.CP
{
    internal class CPSubsystem : ICPSubsystem
    {
        private readonly DistributedObjectFactory _factory;

        public CPSubsystem(DistributedObjectFactory factory, CPSessionManager cpSessionManager)
        {
            _factory = factory;
        }

        public async Task<IAtomicLong> GetAtomicLongAsync(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            return await _factory.GetOrCreateAsync<AtomicLong>(ServiceNames.AtomicLong, name).CAF();
        }

        public async Task<IAtomicReference<T>> GetAtomicReferenceAsync<T>(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            return await _factory.GetOrCreateAsync<AtomicReference<T>>(ServiceNames.AtomicReference, name);
        }

        public async Task<ICountdownEvent> GetCountdownEventAsync(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            return await _factory.GetOrCreateAsync<HCountdownEvent>(ServiceNames.CountDownLatch, name).CAF();
        }

        public async Task<ISemaphore> GetSemaphoreAsync(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            return await _factory.GetOrCreateAsync<HSemaphore>(ServiceNames.Semaphore, name).CAF();
        }

        public async Task<IFencedLock> GetLockAsync(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            return await _factory.GetOrCreateAsync<FencedLock>(ServiceNames.FencedLock, name).CAF();
        }
    }
}