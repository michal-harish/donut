# donut
recursive stream processor on top of kafka

> recursvie streaming bsp usecase > custom dual offset > custom rebalance logic

> custom HashMap of linked lz4 blocks with parallel expiring structure of uncompressed blocks


TODO
--------------------
- SYS-5029 Kafka8 Brokers config: log.cleaner.enable=true
- KafkaRangePartitioner base on positive int32 cardinality, tested by the same rules as HBase RegionPartitioner
- LZ4ConcurrentHashMap



KAFKA ADMIN
--------------------
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com --delete --topic graphstream
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com --delete --topic graphstate

./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com  --create --topic graphstream --partitions 24 --replication-factor 1 --config cleanup.policy=delete
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com  --create --topic graphstate --partitions 24 --replication-factor 1 --config cleanup.policy=compact