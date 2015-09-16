package org.apache.donut

import java.util.concurrent.{TimeUnit, Executors}

import kafka.api.FetchResponse
import kafka.common.{TopicAndPartition, ErrorMapping}
import org.apache.hadoop.conf.Configuration


/**
 * Created by mharis on 14/09/15.
 *
 * The task may either be run in an executor or yarn container
 * @param config - initial configuration for the entire application
 * @param logicalPartition - the index of logical partition for this task
 * @param totalLogicalPartitions - number of logical partitions determined by the DonutApp
 * @param topics - list of topics to consume
 *
 */

abstract class DonutAppTask(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends Runnable {

  protected val kafkaUtils = KafkaUtils(config)

  private val topicPartitions: Map[String, Int] = kafkaUtils.getPartitionMap(topics)

  protected val partitionsToConsume: Map[String, Seq[Int]] = topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % totalLogicalPartitions == logicalPartition))
  }}

  protected def awaitingTermination

  protected def onShutdown

  protected def createFetcher(topic: String, partition: Int, groupId: String): Runnable

  final override def run: Unit = {
    println(s"Starting Donut Task for logical partition ${logicalPartition}/${totalLogicalPartitions}")
    val fetchers = partitionsToConsume.flatMap {
      case (topic, partitions) => partitions.map(partition => {
        createFetcher(topic, partition, config.get("kafka.group.id"))
      })
    }
    val executor = Executors.newFixedThreadPool(fetchers.size)
    fetchers.foreach(fetcher => executor.submit(fetcher))
    executor.shutdown
    try {
      while (!Thread.interrupted()) {
        if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
          return
        } else {
          awaitingTermination
        }
      }
    } finally {
      onShutdown
    }
  }


}