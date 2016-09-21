#!/bin/bash
set -eu

#send 10 message per second
echo "textstreaming start"
while true; do
python /home/ubuntu/insight/kafka/producer.py
sleep .1
done
