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

namespace Hazelcast.CP
{
    internal sealed class RaftGroupId : ICPGroupId, IEquatable<RaftGroupId>
    {
        public RaftGroupId(string name, long seed, long groupId)
        {
            Name = name;
            Seed = seed;
            Id = groupId;
        }

        public long Id { get; }

        public long Seed { get; }

        public string Name { get; }

        public bool Equals(RaftGroupId other)
        {
            if (other is null) return false;
            return ReferenceEquals(this, other) || EqualsN(this, other);
        }

        public override bool Equals(object other)
        {
            if (ReferenceEquals(this, other)) return true;
            return other is RaftGroupId thing && EqualsN(this, thing);
        }

        public static bool Equals(RaftGroupId left, RaftGroupId right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (left is null || right is null) return false;
            return EqualsN(left, right);
        }

        private static bool EqualsN(RaftGroupId left, RaftGroupId right)
            => left.Id == right.Id &&
               left.Seed == right.Seed &&
               left.Name == right.Name;

        public override int GetHashCode()
        {
            unchecked
            {
                //name is not null
                var hashCode = Name.GetHashCode(StringComparison.Ordinal);
                hashCode = (hashCode * 31) ^ Seed.GetHashCode();
                hashCode = (hashCode * 31) ^ Id.GetHashCode();
                return hashCode;
            }
        }

        public override string ToString() => $"ICPGroupId(Name={Name}, Seed={Seed}, GroupId={Id})";
    }
}
