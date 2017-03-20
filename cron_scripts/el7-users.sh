#!/bin/bash
output=$(/usr/bin/python /home/egrasmick/el7-users.py)
/bin/echo "$output"
claimed=$(/bin/echo "$output" | /bin/grep Claimed | /bin/awk '{total = total + $4}END{print total}')
/bin/echo "pools.chtc.slots.el7.claimed $claimed $(/bin/date +%s)" | /usr/bin/nc -w30 -v localhost 2003
unclaimed=$(/bin/echo "$output" | /bin/grep Unclaimed | /bin/awk '{total = total + $4}END{print total}')
/bin/echo "pools.chtc.slots.el7.unclaimed $unclaimed $(/bin/date +%s)" | /usr/bin/nc -w30 -v localhost 2003
owner=$(/bin/echo "$output" | /bin/grep Owner | /bin/awk '{total = total + $4}END{print total}')
/bin/echo "pools.chtc.slots.el7.owner $owner $(/bin/date +%s)" | /usr/bin/nc -w30 -v localhost 2003
users=$(/bin/echo "$output" | /bin/grep Claimed | /bin/awk '{print $1, $2, $3, $4}')
each=$(/bin/echo "$users" | /bin/awk -F ' ' '{a[$2] += $4} END{for (i in a) print i, a[i]}')
while IFS=$'\n' read line; do
    /bin/echo $line
    user=$(/bin/echo $line | /bin/awk '{print $1}')
    cores=$(/bin/echo $line | /bin/awk '{print $2}')
    /bin/echo "pools.chtc.slots.el7.users.$user $cores $(/bin/date +%s)"
    /bin/echo "pools.chtc.slots.el7.users.$user $cores $(/bin/date +%s)" | /usr/bin/nc -w30 -v localhost 2003
done <<< "$each"
