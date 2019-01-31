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

using System;
using System.Collections.Generic;
using System.Linq;
using Hazelcast.Client.Proxy;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.IO;
using Hazelcast.Test;
using NUnit.Framework;

namespace Hazelcast.Client.Test
{
    [TestFixture]
    public class ClientPNCounterTest : MultiMemberBaseTest
    {
        [SetUp]
        public void Setup()
        {
           
        }

        [TearDown]
        public void TearDown()
        {
           
        }

        protected override void ConfigureGroup(ClientConfig config)
        {
            config.GetGroupConfig().SetName(HzCluster.Id).SetPassword(HzCluster.Id);
        }

        protected override void ConfigureClient(ClientConfig config)
        {
            //config.GetNetworkConfig().AddAddress("localhost:5701");
            config.GetNetworkConfig().SetConnectionAttemptLimit(1);
            config.GetNetworkConfig().SetConnectionAttemptPeriod(2000);

            //base.ConfigureClient(config);
        }

        protected override string GetServerConfig()
        {
            return Resources.hazelcast_quick_node_switching;
        }

        [OneTimeTearDown]
        public void RestoreEnvironmentVariables()
        {
        }

        private HazelcastClient GetClient()
        {
            return ((HazelcastClientProxy)CreateClient()).GetClient(); ;
        }

        private ClientPNCounterProxy GetPNCounterProxy()
        {
            return GetClient().GetPNCounter(TestSupport.RandomString()) as ClientPNCounterProxy;
        }
        private ClientPNCounterProxy GetPNCounterProxy(IHazelcastInstance client)
        {
            return client.GetPNCounter(TestSupport.RandomString()) as ClientPNCounterProxy;
        }

        [Test]
        public void Reset_Succeeded()
        {
            GetPNCounterProxy().Reset();
        }

        [Test]
        public void AddAndGet_Succeeded()
        {
            var result = GetPNCounterProxy().AddAndGet(10);
            Assert.AreEqual(10, result);
        }

        [Test]
        public void DecrementAndGet_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result = inst.DecrementAndGet();

            Assert.AreEqual(9, result);
        }

        [Test]
        public void Get_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);
            var result = inst.Get();

            Assert.AreEqual(10, result);
        }

        [Test]
        public void GetAndAdd_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result1 = inst.GetAndAdd(10);
            var result2 = inst.Get();

            Assert.AreEqual(result1+10, result2);
        }

        [Test]
        public void GetAndDecrement_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result1 = inst.GetAndDecrement();
            var result2 = inst.Get();

            Assert.AreEqual(result1, result2 + 1);
        }

        [Test]
        public void GetAndIncrement_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result1 = inst.GetAndIncrement();
            var result2 = inst.Get();

            Assert.AreEqual(result1 + 1, result2);
        }

        [Test]
        public void GetAndSubtract_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result1 = inst.GetAndSubtract(5);
            var result2 = inst.Get();

            Assert.AreEqual(result1, result2 + 5);
        }

        [Test]
        public void IncrementAndGet_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result1 = inst.IncrementAndGet();
            var result2 = inst.Get();

            Assert.AreEqual(result1, result2);
            Assert.AreEqual(11, result2);
        }

        [Test]
        public void SubtractAndGet_Succeeded()
        {
            var inst = GetPNCounterProxy();
            inst.AddAndGet(10);

            var result1 = inst.SubtractAndGet(5);
            var result2 = inst.Get();

            Assert.AreEqual(result1, result2);
            Assert.AreEqual(5, result2);
        }

        [Test]
        public void UpdateObservedReplicaTimestamps_Later_Succeeded()
        {
            var inst = GetPNCounterProxy();

            var initList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 20),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            inst.UpdateObservedReplicaTimestamps(initList);

            var testList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 50),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            inst.UpdateObservedReplicaTimestamps(testList);
        }

        [Test]
        public void UpdateObservedReplicaTimestamps_Earlier_Succeeded()
        {
            var inst = GetPNCounterProxy();

            var initList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 20),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            inst.UpdateObservedReplicaTimestamps(initList);

            var testList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 10),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            inst.UpdateObservedReplicaTimestamps(testList);
        }

        [Test]
        public void InvokeAdd_NoAddressNoLastException_ThrowsDefaultException()
        {
            var inst = GetPNCounterProxy();
            var excludedAddresses=new HashSet<Address>();
            Exception lastException = null;
            Address targetAddress = null;

            var ex = Assert.Throws<NoDataMemberInClusterException>(() => inst.InvokeAdd(10, true, excludedAddresses, lastException, targetAddress));
            //Assert.That(ex.Message, Is.EqualTo("Actual exception message"));
        }

        [Test]
        public void InvokeAdd_NoAddressHasLastException_ThrowsLastException()
        {
            var inst = GetPNCounterProxy();
            var excludedAddresses = new HashSet<Address>();
            Exception lastException = new OutOfMemoryException();
            Address targetAddress = null;

            var ex = Assert.Throws<OutOfMemoryException>(() => inst.InvokeAdd(10, true, excludedAddresses, lastException, targetAddress));
        }

        [Test]
        public void InvokeGet_NoAddressNoLastException_ThrowsDefaultException()
        {
            var excludedAddresses = new HashSet<Address>();
            Exception lastException = null;
            Address targetAddress = null;

            // Get PNCounter instance
            var client = CreateClient();
            var inst = GetPNCounterProxy(client);

            var ex = Assert.Throws<NoDataMemberInClusterException>(() => inst.InvokeGet(excludedAddresses, lastException, targetAddress));
            //Assert.That(ex.Message, Is.EqualTo("Actual exception message"));
        }

        [Test]
        public void InvokeGet_NoAddressHasLastException_ThrowsLastException()
        {
            var excludedAddresses = new HashSet<Address>();
            Exception lastException = new OutOfMemoryException();
            Address targetAddress = null;

            // Get PNCounter instance
            var client = CreateClient();
            var inst = GetPNCounterProxy(client);

            var ex = Assert.Throws<OutOfMemoryException>(() => inst.InvokeGet(excludedAddresses, lastException, targetAddress));
        }
    }
}
