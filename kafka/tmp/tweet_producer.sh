#!/bin/bash
set -eu

#send 10 message per second
echo "textstreaming start"
while true; do
python /home/ubuntu/insight/kafka/tweet_producer.py
sleep .01
done
