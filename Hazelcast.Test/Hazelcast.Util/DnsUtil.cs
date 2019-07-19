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
using System.Net;

namespace Hazelcast.Util
{
    /// <summary>
    /// A simple Util class enabling fault injection for <see cref="Dns"/> method calls.
    /// </summary>
    internal static partial class DnsUtil
    {

        public static class Overrides
        {
            public static IDisposable GetHostName(Func<string> getHostName)
            {
                var f = _getHostNameFunc;
                _getHostNameFunc = getHostName;
                return new Disposable(() => { _getHostNameFunc = f; });
            }

            public static IDisposable GetHostEntry(Func<string, IPHostEntry> getHostEntry)
            {
                var f = _getHostEntryFunc;
                _getHostEntryFunc = getHostEntry;
                return new Disposable(() => { _getHostEntryFunc = f; });
            }

            public static IDisposable GetHostAddresses(Func<string, IPAddress[]> getHostAddresses)
            {
                var f = _getHostAddressesFunc;
                _getHostAddressesFunc = getHostAddresses;
                return new Disposable(() => { _getHostAddressesFunc = f; });
            }

            class Disposable : IDisposable
            {
                readonly Action _action;

                public Disposable(Action action)
                {
                    _action = action;
                }

                public void Dispose()
                {
                    _action();
                }
            }
        }
    }
}