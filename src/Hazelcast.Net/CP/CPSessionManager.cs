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

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Hazelcast.Clustering;
using Hazelcast.Core;
using Hazelcast.Protocol;
using Hazelcast.Protocol.Codecs;
using Hazelcast.Protocol.Data;
using Microsoft.Extensions.Logging;

namespace Hazelcast.CP
{
    internal class CPSessionManager : IAsyncDisposable
    {
        private readonly Cluster _cluster;

        /// <summary>
        /// Represents absence of a Raft session
        /// </summary>
        public const long NoSessionId = -1;
        private readonly ILogger<Heartbeat> _logger;

        private readonly ConcurrentDictionary<RaftGroupId, ValueTask<SessionState>> _sessions =
            new ConcurrentDictionary<RaftGroupId, ValueTask<SessionState>>();

        private readonly ConcurrentAsyncDictionary<(RaftGroupId, long), long> _threadIds =
            new ConcurrentAsyncDictionary<(RaftGroupId, long), long>();

        // private final AtomicBoolean scheduleHeartbeat = new AtomicBoolean(false);
        // private final ReadWriteLock lock = new ReentrantReadWriteLock();
        // private boolean running = true;

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private volatile bool _disposed;
        
        private Task _heartbeatTask;
        private int _heartbeating;


        public CPSessionManager(Cluster cluster, ILoggerFactory loggerFactory)
        {
            _cluster = cluster;
            _logger = loggerFactory?.CreateLogger<Heartbeat>() ?? throw new ArgumentNullException(nameof(loggerFactory));
        }

        public async Task<long> GetOrCreateUniqueThreadIdAsync(RaftGroupId raftGroupId)
        {
            _lock.EnterReadLock();
            Task<long> task;
            try
            {
                var key = (raftGroupId, AsyncContext.CurrentContext.Id);
                task = _threadIds.GetOrAddAsync(key, GenerateThreadId, CancellationToken.None).AsTask();
            }
            finally
            {
                _lock.ExitReadLock();
            }

            return await task.CAF();
        }

        private async ValueTask<long> GenerateThreadId((RaftGroupId raftGroupId, long threadId) key,
            CancellationToken cancellationToken)
        {
            var request = CPSessionGenerateThreadIdCodec.EncodeRequest(key.raftGroupId);
            var response = await _cluster.Messaging.SendAsync(request, cancellationToken).CAF();
            return CPSessionGenerateThreadIdCodec.DecodeResponse(response).Response;
        }

        private ValueTask<SessionState> GetOrCreateSession(RaftGroupId groupId)
        {
            _lock.EnterReadLock();
            try
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException("Session manager is already shut down!");
                }

                if (_sessions.TryGetValue(groupId, out var sessionTask) && ValidateSession(sessionTask))
                    return sessionTask;

                lock (groupId)
                {
                    if (_sessions.TryGetValue(groupId, out sessionTask) && ValidateSession(sessionTask))
                        return sessionTask;

                    var newSessionTask = CreateNewRemoteSession(groupId);
                    _sessions.TryAdd(groupId, newSessionTask);
                    // scheduleHeartbeatTask(response.getHeartbeatMillis());
                }

                sessionTask = sessionTask != null ? await sessionTask.CAF() : sessionTask;
                return sessionTask;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        private bool ValidateSession(ValueTask<SessionState> sessionTask) =>
            sessionTask.IsCompleted && sessionTask.Result.IsValid;

        // private SessionState CreateNewSession(RaftGroupId groupId) 
        // {
        //         SessionResponse response = requestNewSession(groupId);
        //         SessionState session = new SessionState(response.getSessionId(), response.getTtlMillis());
        //         sessions.put(groupId, session);
        //         scheduleHeartbeatTask(response.getHeartbeatMillis());
        //         return session;
        // }

        private async ValueTask<SessionState> CreateNewRemoteSession(RaftGroupId raftGroupId,
            CancellationToken cancellationToken = default)
        {
            var request = CPSessionCreateSessionCodec.EncodeRequest(raftGroupId, _cluster.ClientName);
            var resp = await _cluster.Messaging.SendAsync(request, cancellationToken).CAF();
            var respParam = CPSessionCreateSessionCodec.DecodeResponse(resp);
            var heartbeatInterval = TimeSpan.FromMilliseconds(respParam.HeartbeatMillis);
            await ScheduleHeartbeatTask(heartbeatInterval).CAF();
            return new SessionState(respParam.SessionId, TimeSpan.FromMilliseconds(respParam.TtlMillis));
        }


        private async Task ScheduleHeartbeatTask(TimeSpan heartbeatInterval)
        {
            if (Interlocked.CompareExchange(ref _heartbeating, 1, 0) == 0)
            {
                _heartbeatTask ??= HeartbeatLoopAsync(heartbeatInterval);
            }
        }

        private async Task HeartbeatLoopAsync(TimeSpan heartbeatInterval)
        {
            while (true)
            {
                foreach (var (groupId, sessionTask) in _sessions)
                {
                    var session = await sessionTask.CAF();
                    if (session.IsInUse)
                    {
                        try
                        {
                            await HeartbeatAsync(groupId, session.Id).CAF();
                        }
                        catch (RemoteException e) when (e.Error == RemoteError.SessionExpiredException ||
                                                        e.Error == RemoteError.CpGroupDestroyedException)
                        {
                        }
                    }
                }
                await Task.Delay(heartbeatInterval).CAF();
            }
        }

        private async Task HeartbeatAsync(RaftGroupId groupId, long sessionId)
        {
            throw new NotImplementedException();
        }

        private async ValueTask ShutdownAsync()
        {
        }

        public async ValueTask DisposeAsync()
        {
            //TODO SHUTDOWN
            _lock.EnterWriteLock();
            try
            {
                _disposed = true;
                // await ShutdownAsync();
                
                //shutdown heartbeat
                try
                {
                    await _heartbeatTask.CAF();
                    _heartbeatTask = null;
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception e)
                {
                    // unexpected
                    _logger.LogWarning(e, "Heartbeat has thrown an exception.");
                }
                
                
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
    }
}