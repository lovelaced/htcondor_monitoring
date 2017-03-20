#!/bin/bash
gpuinfo=$(/usr/bin/condor_status -const 'partitionableslot==True && DetectedGpus>0' -af detectedgpus gpus machine | /bin/awk '{print $3, $1 - $2, $1}')
owners=$(/usr/bin/condor_status -const 'SlotType=="Dynamic" && GPUs>0' -af machine GPUs RemoteOwner | /bin/awk '{print $2, $1, $3}')
owner=$(/bin/echo "$owners" | /bin/cut -d "@" -f 2 | cut -d "." -f 1 | sort | uniq -c)
declare -A OWNERS
while IFS=$'\n' read line; do
    domain=$(/bin/echo $line | /bin/awk {'print $3'} | cut -d "." -f 1 | cut -d "@" -f 2 )
    count=$(/bin/echo $line | /bin/awk {'print $1'})
    let "OWNERS[$domain]+=$count"
done <<< "$owners"
for domain in "${!OWNERS[@]}"; do
    count=${OWNERS[$domain]}
    /bin/echo "pools.chtc.slots.gpus.by_domain.$domain $count $(/bin/date +%s)" #| /usr/bin/nc -w10 localhost 2003
done
while IFS=$'\n' read line; do
    #/bin/echo $line
    machine=$(/bin/echo $line | /bin/cut -d "." -f 1)
    gpusinuse=$(/bin/echo $line | /bin/awk {'print $2'})
    detectedgpus=$(/bin/echo $line | /bin/awk {'print $3'})
    /bin/echo "pools.chtc.slots.gpus.detected.$machine $detectedgpus $(/bin/date +%s)" #| /usr/bin/nc -w10 localhost 2003
    /bin/echo "pools.chtc.slots.gpus.inuse.$machine $gpusinuse $(/bin/date +%s)" #| /usr/bin/nc -w10 localhost 2003
done <<< "$gpuinfo"
