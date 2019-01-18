// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

using Hazelcast.Core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hazelcast.Client.Test
{
    [TestFixture]
    public class ClientPNCounterTest : SingleMemberBaseTest
    {
        internal static IPNCounter _inst;
        internal const string name = "ClientPNCounterTest";

        [SetUp]
        public void Init()
        {
            _inst = Client.GetPNCounter(TestSupport.RandomString());
        }

        [TearDown]
        public static void Destroy()
        {
            _inst.Destroy();
        }

        [Test]
        public void Test()
        {

        }
    }
}
