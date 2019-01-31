/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Hazelcast.Client.Protocol.Codec;
using Hazelcast.Client.Spi;
using Hazelcast.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using Hazelcast.Net.Ext;
using Hazelcast.IO;
using Hazelcast.Client.Protocol;
using Hazelcast.Logging;

using TimeStampIList = System.Collections.Generic.IList<System.Collections.Generic.KeyValuePair<string, long>>;

namespace Hazelcast.Client.Proxy
{
    internal class ClientPNCounterProxy : ClientProxy, IPNCounter
    {
        internal static readonly HashSet<Address> _emptyAddressList = new HashSet<Address>();

        private AtomicReference<VectorClock> _observedClock;
        private readonly AtomicInteger _maxConfiguredReplicaCount;
        private readonly object _targetAddressGuard = new object();

        // Exposed for unit tests
        internal Address _currentTargetReplicaAddress;

        public ClientPNCounterProxy(string serviceName, string objectId) : base(serviceName, objectId)
        {
            _observedClock = new AtomicReference<VectorClock>(new VectorClock());
            _maxConfiguredReplicaCount = new AtomicInteger();
        }

        public void UpdateObservedReplicaTimestamps(TimeStampIList timeStamps)
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
            // Ensure the current address is not on excluded addresses list
            if (_currentTargetReplicaAddress != null  && !excludedAddresses.Contains(_currentTargetReplicaAddress))
                return _currentTargetReplicaAddress;

            // If address has not been provided or is on exclusion list
            lock (_targetAddressGuard)
            {
                if (_currentTargetReplicaAddress == null || excludedAddresses.Contains(_currentTargetReplicaAddress))
                    _currentTargetReplicaAddress = ChooseTargetReplica(excludedAddresses);
            }

            return _currentTargetReplicaAddress;
        }

        private Address ChooseTargetReplica(HashSet<Address> excludedAddresses)
        {
            var replicaAddresses = GetReplicaAddresses(excludedAddresses);
            if (replicaAddresses.Count==0)
                return null;

            // Choose random replica
            int randomReplicaIndex = new Random().Next(replicaAddresses.Count);
            return replicaAddresses[randomReplicaIndex];
        }

        private int GetMaxConfiguredReplicaCount()
        {
            if (_maxConfiguredReplicaCount.Get() > 0)
                return _maxConfiguredReplicaCount.Get();

            var request = PNCounterGetConfiguredReplicaCountCodec.EncodeRequest(GetName());
            var response = Invoke(request);
            var decodedResult = PNCounterGetConfiguredReplicaCountCodec.DecodeResponse(response);

            _maxConfiguredReplicaCount.Set(decodedResult.response);
            return _maxConfiguredReplicaCount.Get();
        }

        private List<Address> GetReplicaAddresses(HashSet<Address> excludedAddresses)
        {
            var dataMembers = GetContext().GetClusterService().GetMemberList().Where(x=>!x.IsLiteMember).ToList();
            var maxConfiguredReplicaCount = GetMaxConfiguredReplicaCount();
            int currentReplicaCount = Math.Min(maxConfiguredReplicaCount, dataMembers.Count);
            var replicaAddresses = dataMembers
                .Select(x => x.GetAddress())
                .Where(x => excludedAddresses.Contains(x) == false)
                .Take(currentReplicaCount)
                .ToList();
          
            return replicaAddresses;
        }

        public IClientMessage InvokeAdd(long delta, bool getBeforeUpdate, HashSet<Address> excludedAddresses, Exception lastException, Address targetAddress)
        {
            if (targetAddress == null)
            {
                if (lastException != null)
                    throw lastException;

                throw new NoDataMemberInClusterException("Cannot invoke operations on a CRDT because the cluster does not contain any data members");
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

                // Add current/failed address to exclusion list
                excludedAddresses.Add(targetAddress);

                // Look for the new target address (taking into account exclusion list)
                var newTarget = GetCRDTOperationTarget(excludedAddresses);

                // Send null target address in case it's uninitialized instance
                return InvokeAdd(delta, getBeforeUpdate, excludedAddresses, ex, newTarget.GetPort() == -1 ? null : newTarget);
            }
        }

        public IClientMessage InvokeGet(HashSet<Address> excludedAddresses, Exception lastException, Address targetAddress)
        {
            if (targetAddress == null)
            {
                if (lastException != null)
                    throw lastException;

                throw new NoDataMemberInClusterException("Cannot invoke operations on a CRDT because the cluster does not contain any data members");
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

                // Add current/failed address to exclusion list
                excludedAddresses.Add(targetAddress);

                // Look for the new target address (taking into account exclusion list)
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
