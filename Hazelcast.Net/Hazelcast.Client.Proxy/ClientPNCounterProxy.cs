using Hazelcast.Client.Protocol.Codec;
using Hazelcast.Client.Spi;
using Hazelcast.Core;
using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading;
using Hazelcast.Net.Ext;
using Hazelcast.IO;

using TimeStampIList = System.Collections.Generic.IList<System.Collections.Generic.KeyValuePair<string, long>>;
using Hazelcast.Client.Protocol;

namespace Hazelcast.Client.Proxy
{
    internal class ClientPNCounterProxy : ClientProxy, IPNCounter
    {
        private static readonly HashSet<Address> _emptyAddressList = new HashSet<Address>();

        private Address _currentTargetReplicaAddress;
        private AtomicReference<VectorClock> _observedClock;

        public ClientPNCounterProxy(string serviceName, string objectId) : base(serviceName, objectId)
        {
            _observedClock = new AtomicReference<VectorClock>(new VectorClock());
        }

        private void UpdateObservedReplicaTimestamps(TimeStampIList timeStamps)
        {
            var newVectorClock = new VectorClock(timeStamps);
            while (true)
            {
                if (_observedClock.Get().IsAfter(newVectorClock))
                    if (_observedClock.CompareAndSet(_observedClock.Get(), newVectorClock))
                        break;
            }
        }

        public long AddAndGet(long delta)
        {
            var target = GetCRDTOperationTarget(_emptyAddressList);

            var request = PNCounterAddCodec.EncodeRequest(GetName(), delta, false, _observedClock.Get().TimeStampList, _currentTargetReplicaAddress);
            var response = InvokeAdd(request);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        private IClientMessage InvokeAdd(ClientMessage request)
        {
            // It's trying to run request on random node.
            // In case it failed it choses next one available from th list and adds failed node to disabled targets list
            throw new NotImplementedException();
        }

        private IClientMessage InvokeGet(ClientMessage request)
        {
            throw new NotImplementedException();
        }

        private Address GetCRDTOperationTarget(HashSet<Address> excludedAddresses)
        {
            throw new NotImplementedException();
        }

        public long DecrementAndGet()
        {
            throw new NotImplementedException();
        }

        public long Get()
        {
            throw new NotImplementedException();
        }

        public long GetAndAdd(long delta)
        {
            throw new NotImplementedException();
        }

        public long GetAndDecrement()
        {
            throw new NotImplementedException();
        }

        public long GetAndIncrement()
        {
            throw new NotImplementedException();
        }

        public long GetAndSubtract(long delta)
        {
            throw new NotImplementedException();
        }

        public long IncrementAndGet()
        {
            throw new NotImplementedException();
        }

        public long SubtractAndGet(long delta)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            _observedClock = new AtomicReference<VectorClock>(new VectorClock());
        }
    }
}
