#!/bin/bash

iplist=('10.2.6.165' '10.2.6.201' '10.2.7.138' '10.2.7.68')
for ipd in "${iplist[@]}"; do
	pkill -9 cdc
	echo "stopping cdc service for node: $ipd"
	ssh $ipd -- pkill -9 cdc
	ssh $ipd -- systemctl stop cdc-8300
done

for ipd in "${iplist[@]}"; do
	echo "coping new cdc binary to node: $ipd"
	scp bin/cdc $ipd:/home/root/deploy/cdc-8300/bin/cdc
done

for ipd in "${iplist[@]}"; do
	echo "starting cdc service for node: $ipd"
	ssh $ipd -- systemctl start cdc-8300
done
