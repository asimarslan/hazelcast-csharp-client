using Hazelcast.Core;
using NUnit.Framework;
using System.Collections.Generic;

namespace Hazelcast.Client.Test
{
    public class VectorClockTest : SingleMemberBaseTest
    {
        internal static VectorClock _inst;
        internal const string name = "ClientPNCounterTest";

        [SetUp]
        public void Init()
        {
            var initList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 20),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            _inst = new VectorClock(initList);
        }

        [TearDown]
        public static void Destroy()
        {
            _inst = null;
        }

        [Test]
        public void NewerTSDetectedOnNewSet()
        {
            // Arrange
            var newList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 100),
                new KeyValuePair<string, long>("node-2", 20),
                new KeyValuePair<string, long>("node-3", 30),
                new KeyValuePair<string, long>("node-4", 40),
                new KeyValuePair<string, long>("node-5", 50)
            };

            var newVector = new VectorClock(newList);

            // Act
            var result = _inst.IsAfter(newVector);

            // Assert
            Assert.IsFalse(result);
        }

        [Test]
        public void SmallerListOnNewSet()
        {
            // Arrange
            var newList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 10),
                new KeyValuePair<string, long>("node-2", 20)
            };

            var newVector = new VectorClock(newList);

            // Act
            var result = _inst.IsAfter(newVector);

            // Assert
            Assert.IsTrue(result);
        }

        [Test]
        public void SmallerListWithNewerItemOnNewSet()
        {
            // Arrange
            var newList = new List<KeyValuePair<string, long>>()
            {
                new KeyValuePair<string, long>("node-1", 100),
                new KeyValuePair<string, long>("node-2", 20)
            };

            var newVector = new VectorClock(newList);

            // Act
            var result = _inst.IsAfter(newVector);

            // Assert
            Assert.IsFalse(result);
        }
    }
}
