@echo off
setlocal EnableDelayedExpansion
pushd %~dp0

ECHO STARTING
FOR /L %%V IN (1,1,5) DO (
    ECHO %%V
    start "hazelcast-client-%%V net40" cmd /c "bin\Release\net40\Hazelcast.Examples.exe 10.212.1.101:5701 10.212.1.101:5702 10.212.1.102:5701 10.212.1.102:5702 > client_stdout-%%V-net40.txt 2>client_stderr-%%V-net40.txt
)

FOR /L %%V IN (1,1,5) DO (
    ECHO %%V
    start "hazelcast-client-%%V Netcore" cmd /c "dotnet bin\Release\netcoreapp2.0\Hazelcast.Examples.dll 10.212.1.101:5701 10.212.1.101:5702 10.212.1.102:5701 10.212.1.102:5702 > client_stdout-%%V-netcoreapp.txt 2>client_stderr-%%V-netcoreapp.txt
)

popd

ECHO END.
