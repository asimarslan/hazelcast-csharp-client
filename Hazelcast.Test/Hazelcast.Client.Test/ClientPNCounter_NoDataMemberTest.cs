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

using System.Linq;
using Hazelcast.Client.Proxy;
using Hazelcast.Config;
using Hazelcast.Core;
using Hazelcast.Test;
using NUnit.Framework;

namespace Hazelcast.Client.Test
{
    [TestFixture]
    public class ClientPNCounter_NoDataMemberTest : MultiMemberBaseTest
    {
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
            return Resources.hazelcast_lite_member;
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
        public void ClientPNCounterNoDataMember()
        {
            // Obtain all the members
            var client = CreateClient();
            var allMembers = client.GetCluster().GetMembers();

            // Shutdown "primary" member
            var primaryMember = allMembers.First();
            //RemoteController.terminateMember(HzCluster.Id, primaryMember.GetUuid());

            // Get PNCounter instance
            var inst = GetPNCounterProxy(client);

            // Mutate counter
            var ex = Assert.Throws<NoDataMemberInClusterException>(() => inst.AddAndGet(5));
        }
    }
}
