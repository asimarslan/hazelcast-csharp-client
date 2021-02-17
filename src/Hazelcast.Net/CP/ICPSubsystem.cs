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

namespace Hazelcast.CP
{
    // ReSharper disable once InconsistentNaming
    public interface ICPSubsystem
    {
        Task<IAtomicLong> GetAtomicLongAsync(string name);

        Task<IAtomicReference<T>> GetAtomicReferenceAsync<T>(string name);

        Task<ICountdownEvent> GetCountdownEventAsync(string name);

        Task<ISemaphore> GetSemaphoreAsync(string name);
        
        Task<IFencedLock> GetLockAsync(string name);
    }
}