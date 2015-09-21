# Donut - Recursive Streaming Processor
(Alternative name: *Strudel*)

This is a stream processing *playground* which shares many concepts and ideas with [Apache Samza](http://samza.apache.org/), but this prototype has been developed with recurisve usecase which we found difficult to implement with Samza.


- logical partitionining and stream cogroup
- Kafka log compaction topics for fault-tolerant state management
- Framework provides custom Kafka consumer group implementation deployed in YARN


1. [Desgin](#design)
2. [Configuration](#configuration) 	
3. [Operations](#operations)
4. [Development](#development)

<a name="design">
##Design 		 
</a>

**TODO Donut Application Architecture ... Pipeline, Component, Processing Unit, Logical Partition**

- Local State is purely in memory without replication and relies by design on state bootstrap from compacted topic. (The idea is to have hot regions of the local state to be accessible in constant time as normal concurrent hash map and have the cold regions sorted and  compressed lz4 with bloom filter index and have the oldest lz4 blocks evicted)
- It is fixed to at-least-once guarantee requiring fully idempotent application design
- It is more focused on iterative graph processing algorithms and the starting point for it was the [Connected Components](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) algorithm, more specifically it's [BSP](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel) equivalent.. this prototype is under net.imagini.dxp.

![](doc/Donut_LocalState.png)

<a name="configuration">
## Configuration
</a>

Each component of a pipeline has to be configured by at least the following parameters. Some of these will typically be shared across multiple components of the pipeline which can be loaded from the environment and some will be decisions made by the component implementation, e.g. kafka.brokers will be most likely same for all components but only long-running streaming components will need keepContainers=true,...

paramter|default|description
--------|-------|-----------
**kafka.brokers**| - | Coma-separated list of kafka broker addresses 
**kafka.group.id** | - | Consumer group for the total set of all kafka partitions
**kafka.topics** | - | Coma-separated list of kafka topics to subscribe to
**kafka.cogroup** | false | If set to `false` the number of logical partitions is defined by the topic with the highest number of partitions. If set to `true` the number of logical partitions will be the Highest Common Factor of the number of partitions in each subscribed topic.  
**yarn1.keepContainers** | `false` | If set to `true` any failed container will be automatically restarted.
*yarn1.site* | `/etc/hadoop` | Local path where the application is launched pointing to yarn (and hdfs-hadoop configuration) files. This path should contain at least these files: `yarn-site.xml`, `hdfs-site.xml`, `core-site.xml`
*yarn1.queue* | - | YARN scheduling queue name
yarn1... | ... | For more Yarn1 optional configurations see [Yarn1 Configuration](https://github.com/michal-harish/yarn1#configuration)

<a name="operations">
## Operations
</a>

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

<a name="development">
## Development
</a>

The project contains one submodule so after cloning you need to run: `git submodule update --init`

### TODOs

- The consumer group currently doesn't implement any rebalance algorithm so either check if the appName is already running on cluster or implement rebalance 
- In the recursive example graphstream emit null messages to clear the connections on eviction
- Bootstrap fetcher could be stopped after it is caught up provided the delta fetcher updates the local state but this requires the DeltaFetcher to move the state topic offset as well
- Integrate librebind for task profiling with jprofiler  
- **LZ4LocalStorage** with 2 regions: 1) hot region of simple concurrent linked hash map 2) sorted lz4 blocks with bloom filter and range available for quick consultation. The hot region is defined by maximum size and when enough keys become eligible for eviction they are compressed into a sorted block and lz4-compressed and moved to the second region. Similarly when the second region reaches it's maximum size the oldest lz4 block is evicted, ultimately freeing memory
- **Expose Local Storage as Key-Value API** and try mapping a Spark RDD onto it
