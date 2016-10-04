#!/bin/bash
set -eu

#send 10 message per second
echo "textstreaming start"
python /home/ubuntu/insight/kafka/produceBids.py \
52.2.60.169:9092 bids 
