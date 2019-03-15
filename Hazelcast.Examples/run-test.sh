#!/usr/bin/env bash


echo "Starting clients"

for i in {1..5}
do
    echo "Starting client-$i"
    dotnet bin/Release/netcoreapp2.0/Hazelcast.Examples.dll 10.212.1.111 > client_stdout-${i}.log 2>client_stderr-${i}.log &
done


