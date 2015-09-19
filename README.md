# Donut Project
recursive stream processor on top of kafka

- at-least-once guarantee with fully idempotent design guidelines 
- recursvie streaming bsp usecase > custom dual offset > custom rebalance logic
- it's very similar to samza 

# Work

- TODO get rid of vdna specific configuration from the code - yarn1 could take --yarnConfigPath and load the files from there  
- TODO bootstrap fetcher could be stopped after it is caught up provided the delta fetcher updates the local state
- TODO librebind + jprofiler 
- TODO before submitting to yarn, check if the appName is already running on cluster 
- TODO LZ4ConcurrentHashMap - linked lz4 blocks with parallel expiring structure of uncompressed blocks


# Kafka admin notes
### Brokers configuration
For state topics we require log cleaner enabled on the brokers

```server.properties
log.cleaner.enable=true
```

### Creating normal topic with retention

```bash
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com  --create --topic graphstream --partitions 24 --replication-factor 1 --config cleanup.policy=delete
```

### Creating a compacted topic
And then creating topic with compact cleanup policy
```bash
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com  --create --topic graphstate --partitions 24 --replication-factor 1 --config cleanup.policy=compact
```

### Deleting topics

```bash
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com --delete --topic graphstream
./bin/kafka-topics.sh --zookeeper message-01.prod.visualdna.com --delete --topic graphstate
```


# Design

![](doc/Donut_LocalState.png)
