#!/bin/bash
set -eu

#send 10 message per second
echo "textstreaming start"
python /home/ubuntu/insight/kafka/produceTweets.py \
/home/ubuntu/data/density.json \
/home/ubuntu/data/wordVecList.txt 52.2.60.169:9092 tweets 100
