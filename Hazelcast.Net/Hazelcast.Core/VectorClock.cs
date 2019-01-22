using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using TimeStampIList = System.Collections.Generic.IList<System.Collections.Generic.KeyValuePair<string, long>>;

namespace Hazelcast.Core
{
    internal class VectorClock
    {
        private TimeStampIList _timeStampList;
        private Lazy<Dictionary<string, long>> _timeStampDictionary;

        public TimeStampIList TimeStampList { get { return _timeStampList; } }

        public VectorClock()
        {
            // Default empty list
            _timeStampList=new List<KeyValuePair<string, long>>();

            // Construct the dictionary only when necessary
            _timeStampDictionary = new Lazy<Dictionary<string, long>>(() =>
            {
                var dict = new Dictionary<string, long>(_timeStampList.Count);

                // Because we use IList interface it can be the case that usage of indexer from this interface
                // is much faster then foreach usage based on enumerator
                for (var elemPos = 0; elemPos < _timeStampList.Count; ++elemPos)
                {
                    var elem = _timeStampList[elemPos];
                    dict.Add(elem.Key, elem.Value);
                }
                return dict;
            });
        }

        public VectorClock(TimeStampIList timeStampList) : this()
        {
            _timeStampList = timeStampList;
        }

        // Check if the received vector has newer data (timestamps)
        internal bool IsAfter(VectorClock newVectorClock)
        {
            // There are no any timestamps yet
            if (_timeStampList.Count == 0)
                return false;

            // We have the same amount of timestamps in both collections so let's find which one has newer items
            var dict = _timeStampDictionary.Value; // Create a dictionary instance on current object in case it doesn't exist yet
            var extDict = newVectorClock._timeStampDictionary.Value; // Create a dictionary on ext elem if doesn't exist yet...

            // Do we have any timestamp newer than that on the newly received list?
            bool anyTimestampGreater = false;

            foreach (var extEntry in extDict)
            {
                // C# 4 yet - old out elems declaration
                long localTSValue;
                var localExists = dict.TryGetValue(extEntry.Key, out localTSValue);

                // In case we have no such element or the local TS is older/smaller than on the cluster 
                // we have to update the local TS copy
                if (localExists == false || localTSValue < extEntry.Value)
                    return false;

                if (localTSValue > extEntry.Value)
                    anyTimestampGreater = true;
            }

            // There is at least one local timestamp greater or local vector clock has additional timestamps
            return anyTimestampGreater || extDict.Count < dict.Count;
        }
    }
}
