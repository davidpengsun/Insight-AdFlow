/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic tweets --partitions 3 --replication-factor 2
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic vectors --partition 3 --replication-factor 2
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic events --partition 3 --replication-factor 2
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic bids --partition 3 --replication-factor 2
