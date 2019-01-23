﻿// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

using System.Collections.Generic;
using Hazelcast.Client.Proxy;
using Hazelcast.Core;
using NUnit.Framework;

using TimeStampIList = System.Collections.Generic.IList<System.Collections.Generic.KeyValuePair<string, long>>;

namespace Hazelcast.Client.Test
{
    [TestFixture]
    public class ClientPNCounterTest : SingleMemberBaseTest
    {
        internal static ClientPNCounterProxy _inst;
        internal const string name = "ClientPNCounterTest";

        [SetUp]
        public void Init()
        {
            _inst = Client.GetPNCounter(TestSupport.RandomString()) as ClientPNCounterProxy;
        }

        [TearDown]
        public static void Destroy()
        {
            _inst.Destroy();
        }

        [Test]
        public void AddAndGet_Succeeded()
        {
            var result = _inst.AddAndGet(10);
            Assert.AreEqual(10, result);
        }

        [Test]
        public void DecrementAndGet_Succeeded()
        {
            _inst.AddAndGet(10);
            var result = _inst.DecrementAndGet();

            Assert.AreEqual(9, result);
        }

        [Test]
        public void Get_Succeeded()
        {
            _inst.AddAndGet(10);
            var result = _inst.Get();

            Assert.AreEqual(10, result);
        }

        [Test]
        public void GetAndAdd_Succeeded()
        {
            _inst.AddAndGet(10);

            var result1 = _inst.GetAndAdd(10);
            var result2 = _inst.Get();

            Assert.AreEqual(result1+10, result2);
        }

        [Test]
        public void GetAndDecrement_Succeeded()
        {
            _inst.AddAndGet(10);

            var result1 = _inst.GetAndDecrement();
            var result2 = _inst.Get();

            Assert.AreEqual(result1, result2 + 1);
        }

        [Test]
        public void GetAndIncrement_Succeeded()
        {
            _inst.AddAndGet(10);

            var result1 = _inst.GetAndIncrement();
            var result2 = _inst.Get();

            Assert.AreEqual(result1 + 1, result2);
        }

        [Test]
        public void GetAndSubtract_Succeeded()
        {
            _inst.AddAndGet(10);

            var result1 = _inst.GetAndSubtract(5);
            var result2 = _inst.Get();

            Assert.AreEqual(result1, result2 + 5);
        }

        [Test]
        public void IncrementAndGet_Succeeded()
        {
            _inst.AddAndGet(10);

            var result1 = _inst.IncrementAndGet();
            var result2 = _inst.Get();

            Assert.AreEqual(result1, result2);
            Assert.AreEqual(11, result2);
        }

        [Test]
        public void SubtractAndGet_Succeeded()
        {
            _inst.AddAndGet(10);

            var result1 = _inst.SubtractAndGet(5);
            var result2 = _inst.Get();

            Assert.AreEqual(result1, result2);
            Assert.AreEqual(5, result2);
        }

        [Test]
        public void UpdateObservedReplicaTimestamps_Later_Succeeded()
        {
            var initList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 20),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            _inst.UpdateObservedReplicaTimestamps(initList);

            var testList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 50),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            _inst.UpdateObservedReplicaTimestamps(testList);
        }

        [Test]
        public void UpdateObservedReplicaTimestamps_Earlier_Succeeded()
        {
            var initList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 20),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            _inst.UpdateObservedReplicaTimestamps(initList);

            var testList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 10),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            _inst.UpdateObservedReplicaTimestamps(testList);
        }
    }
}
