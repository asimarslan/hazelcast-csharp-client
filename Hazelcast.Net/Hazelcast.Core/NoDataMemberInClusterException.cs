// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Hazelcast.Core
{
    /// <summary>Thrown when invoke operations on a CRDT failed because the cluster does not contain any data members.</summary>
    /// <remarks>Thrown when invoke operations on a CRDT failed because the cluster does not contain any data members.</remarks>
    [Serializable]
    public class NoDataMemberInClusterException : InvalidOperationException
    {
        public NoDataMemberInClusterException() : base("Cannot invoke operations on a CRDT because the cluster does not contain any data members")
        {
        }
    }
}
