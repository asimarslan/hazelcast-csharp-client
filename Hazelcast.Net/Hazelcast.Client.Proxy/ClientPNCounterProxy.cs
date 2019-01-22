using Hazelcast.Client.Protocol.Codec;
using Hazelcast.Client.Spi;
using Hazelcast.Core;
using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Hazelcast.Net.Ext;
using Hazelcast.IO;
using Hazelcast.Client.Protocol;
using Hazelcast.Logging;

using TimeStampIList = System.Collections.Generic.IList<System.Collections.Generic.KeyValuePair<string, long>>;

namespace Hazelcast.Client.Proxy
{
    internal class ClientPNCounterProxy : ClientProxy, IPNCounter
    {
        private static readonly HashSet<Address> _emptyAddressList = new HashSet<Address>();

        private Address _currentTargetReplicaAddress;
        private AtomicReference<VectorClock> _observedClock;
        private AtomicInteger _maxConfiguredReplicaCount;

        public ClientPNCounterProxy(string serviceName, string objectId) : base(serviceName, objectId)
        {
            _observedClock = new AtomicReference<VectorClock>(new VectorClock());
            _maxConfiguredReplicaCount = new AtomicInteger();
        }

        private void UpdateObservedReplicaTimestamps(TimeStampIList timeStamps)
        {
            var newVectorClock = new VectorClock(timeStamps);

            while (true)
            {
                if (_observedClock.Get().IsAfter(newVectorClock))
                    break;

                if (_observedClock.CompareAndSet(_observedClock.Get(), newVectorClock))
                    break;
            }
        }

        private Address GetCRDTOperationTarget(HashSet<Address> excludedAddresses)
        {
            // Do not we have the current address on a list of excluded addresses?
            if (_currentTargetReplicaAddress != null  && !excludedAddresses.Contains(_currentTargetReplicaAddress))
                return _currentTargetReplicaAddress;

            if (_currentTargetReplicaAddress == null || excludedAddresses.Contains(_currentTargetReplicaAddress))
                _currentTargetReplicaAddress = ChooseTargetReplica(excludedAddresses);

            return _currentTargetReplicaAddress;
        }

        private Address ChooseTargetReplica(HashSet<Address> excludedAddresses)
        {
            var replicaAddresses = GetReplicaAddresses(excludedAddresses);
            if (replicaAddresses.Count==0)
                return new Address();

            // Choose random replica
            int randomReplicaIndex = new Random().Next(replicaAddresses.Count);
            return replicaAddresses[randomReplicaIndex];
        }

        int GetMaxConfiguredReplicaCount()
        {
            if (_maxConfiguredReplicaCount.Get() > 0)
                return _maxConfiguredReplicaCount.Get();

            var request = PNCounterGetConfiguredReplicaCountCodec.EncodeRequest(GetName());
            var response = Invoke(request);
            var decodedResult = PNCounterGetConfiguredReplicaCountCodec.DecodeResponse(response);

            _maxConfiguredReplicaCount.Set(decodedResult.response);
            return _maxConfiguredReplicaCount.Get();
        }

        List<Address> GetReplicaAddresses(HashSet<Address> excludedAddresses)
        {
            var dataMembers = GetContext().GetClusterService().GetMemberList();
            //cluster::memberselector::MemberSelectors::DATA_MEMBER_SELECTOR);

            var maxConfiguredReplicaCount = GetMaxConfiguredReplicaCount();
            int currentReplicaCount = Math.Min(maxConfiguredReplicaCount, dataMembers.Count);

            var replicaAddresses = dataMembers.Select(x => x.GetAddress()).ToList();
          
            return replicaAddresses;
        }

        private IClientMessage InvokeAdd(long delta, bool getBeforeUpdate, HashSet<Address> excludedAddresses, Exception lastException, Address targetAddress)
        {
            if (targetAddress == null)
            {
                if (lastException != null)
                    throw lastException;

                throw new NoDataMemberInClusterException();
            }

            try
            {
                var request = PNCounterAddCodec.EncodeRequest(GetName(), delta, getBeforeUpdate, _observedClock.Get().TimeStampList, targetAddress);
                return Invoke(request);
            }
            catch (Exception ex)
            {
                Logger.GetLogger(GetType()).Finest("Unable to provide session guarantees when sending operations to " +
                                                   targetAddress.ToString() + ", choosing different target. Cause: " +
                                                   ex.ToString());

                // Make sure that this only affects the local variable of the method
                if (excludedAddresses == _emptyAddressList)
                    excludedAddresses = new HashSet<Address>();

                excludedAddresses.Add(targetAddress);

                var newTarget = GetCRDTOperationTarget(excludedAddresses);

                // Send null target address in case it's uninitialized instance
                return InvokeAdd(delta, getBeforeUpdate, excludedAddresses, ex, newTarget.GetPort() == -1 ? null : newTarget);
            }
        }

        private IClientMessage InvokeGet(HashSet<Address> excludedAddresses, Exception lastException, Address targetAddress)
        {
            if (targetAddress == null)
            {
                if (lastException != null)
                    throw lastException;

                throw new NoDataMemberInClusterException();
            }

            try
            {
                var request = PNCounterGetCodec.EncodeRequest(GetName(), _observedClock.Get().TimeStampList, targetAddress);
                return Invoke(request);
            }
            catch (Exception ex)
            {
                Logger.GetLogger(GetType()).Finest("Unable to provide session guarantees when sending operations to " +
                                                   targetAddress.ToString() + ", choosing different target. Cause: " +
                                                   ex.ToString());

                // Make sure that this only affects the local variable of the method
                if (excludedAddresses == _emptyAddressList)
                    excludedAddresses = new HashSet<Address>();

                excludedAddresses.Add(targetAddress);

                var newTarget = GetCRDTOperationTarget(excludedAddresses);

                // Send null target address in case it's uninitialized instance
                return InvokeGet(excludedAddresses, ex, newTarget.GetPort() == -1 ? null : newTarget);
            }
        }



        public long AddAndGet(long delta)
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(delta, false, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long DecrementAndGet()
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(-1, false, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long Get()
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeGet(_emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterGetCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long GetAndAdd(long delta)
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(delta, true, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long GetAndDecrement()
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(-1, true, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long GetAndIncrement()
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(1, true, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long GetAndSubtract(long delta)
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(-delta, true, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long IncrementAndGet()
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(1, false, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public long SubtractAndGet(long delta)
        {
            var targetAddress = GetCRDTOperationTarget(_emptyAddressList);
            var response = InvokeAdd(-delta, false, _emptyAddressList, null, targetAddress);
            var decodedResponse = PNCounterAddCodec.DecodeResponse(response);

            UpdateObservedReplicaTimestamps(decodedResponse.replicaTimestamps);

            return decodedResponse.value;
        }

        public void Reset()
        {
            _observedClock = new AtomicReference<VectorClock>(new VectorClock());
        }
    }
}
