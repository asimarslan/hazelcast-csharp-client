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

namespace Hazelcast.CP
{
    internal class SessionState
    {
        private int _acquireCount;
        private readonly DateTime _expirationTime;

        public SessionState(long id, TimeSpan timeToLive)
        {
            Id = id;
            _expirationTime = DateTime.UtcNow + timeToLive;
        }

        public long Id { get; }

        public bool IsValid => IsInUse || _expirationTime < DateTime.UtcNow;

        public bool IsInUse => Volatile.Read(ref _acquireCount) > 0;

        public bool IsExpired(DateTime timestamp)
        {
            return timestamp > _expirationTime;
        }

        public long Acquire(int count)
        {
            Interlocked.Add(ref _acquireCount, count);
            return Id;
        }

        public void Release(int count)
        {
            Interlocked.Add(ref _acquireCount, -count);
        }
    }
}