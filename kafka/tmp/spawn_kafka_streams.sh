#!/bin/bash
NUM_SPAWNS=$1
SESSION=$2
PRODUCER_DIR="/home/ubuntu/insight/kafka"
tmux new-session -s $SESSION -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do
    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'bash $PRODUCER_DIR/producer.sh' C-m
done
