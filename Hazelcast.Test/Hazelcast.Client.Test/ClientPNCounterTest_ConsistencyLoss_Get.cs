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
    public class ClientPNCounterTest_ConsistencyLoss_Get : MultiMemberBaseTest
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
        public void ConsistencyLostExceptionIsThrownWhenTargetReplicaDisappears_GetCase()
        {
            // Start additional member
            //RemoteController.startMember(HzCluster.Id);

            var client = CreateClient();
            StartMemberAndWait(client, RemoteController, HzCluster, 2);

            // Obtain all the members
            
            var allMembers = client.GetCluster().GetMembers();

            // Get PNCounter instance
            var inst = GetPNCounterProxy(client);

            // Init the counter value
            var result1 = inst.AddAndGet(5);

            // Shutdown "primary" member
            var currentTarget = inst._currentTargetReplicaAddress;
            var primaryMember = allMembers.First(x => x.GetAddress().Equals(currentTarget));

            RemoteController.terminateMember(HzCluster.Id, primaryMember.GetUuid());

            // Obtain the value from the cluster second time
            var result2 = inst.Get();

            Assert.AreEqual(result1, result2);
        }
    }
}
