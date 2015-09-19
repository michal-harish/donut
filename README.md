# Donut - Recursive Streaming Processor

- This is a stream processing *playground* which shares many concepts and ideas with [Apache Samza](http://samza.apache.org/)
	- logical partitionining and stream cogroup
	- it utilises Kafka log compaction topics
	- it provides local state management
	- it uses Kafka low level consumers/fetcher deployed in YARN
- It is different from Sazmza in:
	- It is a tiny prototype-like codebase which is nice to play with  before implementing Samza applications
	- It provides low-level primitive for recursive stream processing (which can be achieved with samza too by combining some of its own primitives)
	- Local State is purely in memory without replication and relies by design on state bootstrap from compacted topic. (The idea is to have hot regions of the local state to be accessible in constant time as normal concurrent hash map and have the cold regions sorted and  compressed lz4 with bloom filter index and have the oldest lz4 blocks evicted)
	- The YARN deployment is slighly more elegant based on [Yarn1 Project](https://github.com/michal-harish/yarn1) which allows launching distributed application seamlessly from IDE - the launchers are in the `src/test/scala/Launchers.scala` because launching from test packages allows for most of the dependencies to be `provided`
	- The aim is also to potentially expose the local state externally via API
	- It is fixed to at-least-once guarantee requiring fully idempotent application design
	- It is more focused on iterative graph processing algorithms and the starting point for it was the [Connected Components](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) algorithm, more specifically it's [BSP](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel) equivalent.. this prototype is under net.imagini.dxp.

![](doc/Donut_LocalState.png)

# Work

- get rid of VisualDNA specific configuration from the code - yarn1 could take --yarnConfigPath and load the files from there  
- bootstrap fetcher could be stopped after it is caught up provided the delta fetcher updates the local state
- integrate librebind for task profiling with jprofiler 
- before submitting to yarn, check if the appName is already running on cluster 
- **LZ4LocalStorage** with 2 regions: 1) hot region of simple concurrent linked hash map 2) sorted lz4 blocks with bloom filter and range available for quick consultation. The hot region is defined by maximum size and when enough keys become eligible for eviction they are compressed into a sorted block and lz4-compressed and moved to the second region. Similarly when the second region reaches it's maximum size the oldest lz4 block is evicted, ultimately freeing memory
- **Expose Local Storage as Key-Value API** and try mapping a Spark RDD onto it


# Kafka admin notes
### Brokers configuration
For state topics we require log cleaner enabled on the brokers

```server.properties
log.cleaner.enable=true
```

### Creating normal topic with retention

```bash
./bin/kafka-topics.sh --create --topic graphstream --partitions 24 --replication-factor 1 --config cleanup.policy=delete
```

### Creating a compacted topic
And then creating topic with compact cleanup policy
```bash
./bin/kafka-topics.sh --create --topic graphstate --partitions 24 --replication-factor 1 --config cleanup.policy=compact
```

### Deleting topics

```bash
./bin/kafka-topics.sh --delete --topic graphstream
./bin/kafka-topics.sh --delete --topic graphstate
```
