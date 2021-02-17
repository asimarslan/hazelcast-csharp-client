// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

// <auto-generated>
//   This code was generated by a tool.
//     Hazelcast Client Protocol Code Generator
//     https://github.com/hazelcast/hazelcast-client-protocol
//   Change to this file will be lost if the code is regenerated.
// </auto-generated>

#pragma warning disable IDE0051 // Remove unused private members
// ReSharper disable UnusedMember.Local
// ReSharper disable RedundantUsingDirective
// ReSharper disable CheckNamespace

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Hazelcast.Protocol.BuiltInCodecs;
using Hazelcast.Protocol.CustomCodecs;
using Hazelcast.Core;
using Hazelcast.Messaging;
using Hazelcast.Logging;
using Hazelcast.Clustering;
using Hazelcast.Serialization;
using Microsoft.Extensions.Logging;

namespace Hazelcast.Protocol.Codecs
{
    /// <summary>
    /// Acquires the requested amount of permits if available, reducing
    /// the number of available permits. If no enough permits are available,
    /// then the current thread becomes disabled for thread scheduling purposes
    /// and lies dormant until other threads release enough permits.
    ///</summary>
#if SERVER_CODEC
    internal static class SemaphoreAcquireServerCodec
#else
    internal static class SemaphoreAcquireCodec
#endif
    {
        public const int RequestMessageType = 786944; // 0x0C0200
        public const int ResponseMessageType = 786945; // 0x0C0201
        private const int RequestSessionIdFieldOffset = Messaging.FrameFields.Offset.PartitionId + BytesExtensions.SizeOfInt;
        private const int RequestThreadIdFieldOffset = RequestSessionIdFieldOffset + BytesExtensions.SizeOfLong;
        private const int RequestInvocationUidFieldOffset = RequestThreadIdFieldOffset + BytesExtensions.SizeOfLong;
        private const int RequestPermitsFieldOffset = RequestInvocationUidFieldOffset + BytesExtensions.SizeOfGuid;
        private const int RequestTimeoutMsFieldOffset = RequestPermitsFieldOffset + BytesExtensions.SizeOfInt;
        private const int RequestInitialFrameSize = RequestTimeoutMsFieldOffset + BytesExtensions.SizeOfLong;
        private const int ResponseResponseFieldOffset = Messaging.FrameFields.Offset.ResponseBackupAcks + BytesExtensions.SizeOfByte;
        private const int ResponseInitialFrameSize = ResponseResponseFieldOffset + BytesExtensions.SizeOfBool;

#if SERVER_CODEC
        public sealed class RequestParameters
        {

            /// <summary>
            /// CP group id of this ISemaphore instance
            ///</summary>
            public Hazelcast.CP.RaftGroupId GroupId { get; set; }

            /// <summary>
            /// Name of this ISemaphore instance
            ///</summary>
            public string Name { get; set; }

            /// <summary>
            /// Session ID of the caller
            ///</summary>
            public long SessionId { get; set; }

            /// <summary>
            /// ID of the caller thread
            ///</summary>
            public long ThreadId { get; set; }

            /// <summary>
            /// UID of this invocation
            ///</summary>
            public Guid InvocationUid { get; set; }

            /// <summary>
            /// number of permits to acquire
            ///</summary>
            public int Permits { get; set; }

            /// <summary>
            /// Duration to wait for permit acquire
            ///</summary>
            public long TimeoutMs { get; set; }
        }
#endif

        public static ClientMessage EncodeRequest(Hazelcast.CP.RaftGroupId groupId, string name, long sessionId, long threadId, Guid invocationUid, int permits, long timeoutMs)
        {
            var clientMessage = new ClientMessage
            {
                IsRetryable = true,
                OperationName = "Semaphore.Acquire"
            };
            var initialFrame = new Frame(new byte[RequestInitialFrameSize], (FrameFlags) ClientMessageFlags.Unfragmented);
            initialFrame.Bytes.WriteIntL(Messaging.FrameFields.Offset.MessageType, RequestMessageType);
            initialFrame.Bytes.WriteIntL(Messaging.FrameFields.Offset.PartitionId, -1);
            initialFrame.Bytes.WriteLongL(RequestSessionIdFieldOffset, sessionId);
            initialFrame.Bytes.WriteLongL(RequestThreadIdFieldOffset, threadId);
            initialFrame.Bytes.WriteGuidL(RequestInvocationUidFieldOffset, invocationUid);
            initialFrame.Bytes.WriteIntL(RequestPermitsFieldOffset, permits);
            initialFrame.Bytes.WriteLongL(RequestTimeoutMsFieldOffset, timeoutMs);
            clientMessage.Append(initialFrame);
            RaftGroupIdCodec.Encode(clientMessage, groupId);
            StringCodec.Encode(clientMessage, name);
            return clientMessage;
        }

#if SERVER_CODEC
        public static RequestParameters DecodeRequest(ClientMessage clientMessage)
        {
            using var iterator = clientMessage.GetEnumerator();
            var request = new RequestParameters();
            var initialFrame = iterator.Take();
            request.SessionId = initialFrame.Bytes.ReadLongL(RequestSessionIdFieldOffset);
            request.ThreadId = initialFrame.Bytes.ReadLongL(RequestThreadIdFieldOffset);
            request.InvocationUid = initialFrame.Bytes.ReadGuidL(RequestInvocationUidFieldOffset);
            request.Permits = initialFrame.Bytes.ReadIntL(RequestPermitsFieldOffset);
            request.TimeoutMs = initialFrame.Bytes.ReadLongL(RequestTimeoutMsFieldOffset);
            request.GroupId = RaftGroupIdCodec.Decode(iterator);
            request.Name = StringCodec.Decode(iterator);
            return request;
        }
#endif

        public sealed class ResponseParameters
        {

            /// <summary>
            /// true if requested permits are acquired,
            /// false otherwise
            ///</summary>
            public bool Response { get; set; }
        }

#if SERVER_CODEC
        public static ClientMessage EncodeResponse(bool response)
        {
            var clientMessage = new ClientMessage();
            var initialFrame = new Frame(new byte[ResponseInitialFrameSize], (FrameFlags) ClientMessageFlags.Unfragmented);
            initialFrame.Bytes.WriteIntL(Messaging.FrameFields.Offset.MessageType, ResponseMessageType);
            initialFrame.Bytes.WriteBoolL(ResponseResponseFieldOffset, response);
            clientMessage.Append(initialFrame);
            return clientMessage;
        }
#endif

        public static ResponseParameters DecodeResponse(ClientMessage clientMessage)
        {
            using var iterator = clientMessage.GetEnumerator();
            var response = new ResponseParameters();
            var initialFrame = iterator.Take();
            response.Response = initialFrame.Bytes.ReadBoolL(ResponseResponseFieldOffset);
            return response;
        }

    }
}