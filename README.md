# donut
recursive stream processor on top of kafka

> at-least-once guarantee with fully idempotent design guidelines 
> recursvie streaming bsp usecase > custom dual offset > custom rebalance logic

WORK
--------------------
- TODO librebind + jprofiler 
- TODO before submitting to yarn, check if the appName is already running on cluster 
- TODO LZ4ConcurrentHashMap - linked lz4 blocks with parallel expiring structure of uncompressed blocks


KAFKA ADMIN
--------------------
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com --delete --topic graphstream
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com --delete --topic graphstate

./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com  --create --topic graphstream --partitions 24 --replication-factor 1 --config cleanup.policy=delete
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com  --create --topic graphstate --partitions 24 --replication-factor 1 --config cleanup.policy=compact
- SYS-5029 log.cleaner.enable=true
- SYS-5023 git repo sys/scala-deploy to distribute stable dependencies to the cluster