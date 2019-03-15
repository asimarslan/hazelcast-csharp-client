@echo off
setlocal EnableDelayedExpansion
pushd %~dp0

ECHO STARTING
FOR /L %%V IN (1,1,5) DO (
    ECHO %%V
    start "hazelcast-client-%%V net46" cmd /c "bin\Release\net46\Hazelcast.Examples.exe 10.212.1.116:5701 10.212.1.116:5702 10.212.1.117:5701 10.212.1.117:5702 > client_stdout-%%V-net46.txt 2>client_stderr-%%V-net46.txt
)

FOR /L %%V IN (1,1,5) DO (
    ECHO %%V
    start "hazelcast-client-%%V Netcore" cmd /c "dotnet bin\Release\netcoreapp2.0\Hazelcast.Examples.dll 10.212.1.116:5701 10.212.1.116:5702 10.212.1.117:5701 10.212.1.117:5702 > client_stdout-%%V-netcoreapp.txt 2>client_stderr-%%V-netcoreapp.txt
)

popd

ECHO END.
