// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
using Hazelcast.IO.Serialization;

namespace Hazelcast.Examples.Models
{
    public class LogData : IPortable
    {
        public const int ClassId = 22;

        private string _taskId;
        private long _asOfDate = long.MaxValue;
        private Message _message;

        internal LogData()
        {
        }

        public LogData(string taskId, Message message)
        {
            _taskId = taskId;
            _message = message;
        }

        public int GetClassId()
        {
            return ClassId;
        }

        public int GetFactoryId()
        {
            return LogPortableFactory.FactoryId;
        }

        public void ReadPortable(IPortableReader reader)
        {
            _taskId = reader.ReadUTF("taskId");
            _asOfDate = reader.ReadLong("asOfDate");
            _message = reader.ReadPortable<Message>("message");
        }

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteUTF("taskId", _taskId);
            writer.WriteLong("asOfDate", _asOfDate);
            writer.WritePortable("message", _message);
        }
    }

    public class Message : IPortable
    {
        public const int ClassId = 11;

        private long _dateTime = long.MaxValue;
        private string _exceptionText;
        private string _logLevel;
        private string _messageText;
        private string _logger;
        private string _source;
        private int _index;

        internal Message()
        {
        }

        public Message(string exceptionText, string logLevel, string messageText, string logger, string source, int index)
        {
            _exceptionText = exceptionText;
            _logLevel = logLevel;
            _messageText = messageText;
            _logger = logger;
            _source = source;
            _index = index;
        }

        public int GetClassId()
        {
            return ClassId;
        }

        public int GetFactoryId()
        {
            return LogPortableFactory.FactoryId;
        }

        public void ReadPortable(IPortableReader reader)
        {
            _dateTime = reader.ReadLong("date");
            _exceptionText = reader.ReadUTF("exception");
            _logLevel = reader.ReadUTF("log");
            _messageText = reader.ReadUTF("message");
            _logger = reader.ReadUTF("logger");
            _source = reader.ReadUTF("source");
            _index = reader.ReadInt("index");
        }

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteLong("date", _dateTime);
            writer.WriteUTF("exception", _exceptionText);
            writer.WriteUTF("log", _logLevel);
            writer.WriteUTF("message", _messageText);
            writer.WriteUTF("logger", _logger);
            writer.WriteUTF("source", _source);
            writer.WriteInt("index", _index);
        }
    }

    internal class LogPortableFactory : IPortableFactory
    {
        public const int FactoryId = 99;

        public IPortable Create(int classId)
        {
            switch (classId)
            {
                case Message.ClassId:
                    return new Message();
                case LogData.ClassId:
                    return new LogData();
                default:
                    throw new ArgumentException("Unknown class id " + classId);
            }
        }
    }
}