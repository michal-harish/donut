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

  protected def asyncProcessMessage(messageAndOffset: kafka.message.MessageAndOffset)

  protected def createFetcher(topic: String, partition: Int, groupId: String): Runnable

  final override def run: Unit = {
    println(s"Starting task for logical partition ${logicalPartition}/${totalLogicalPartitions}")
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

  abstract class Fetcher(topic: String, partition: Int, groupId: String) extends Runnable {
    protected val topicAndPartition = new TopicAndPartition(topic, partition)
    protected var consumer = new kafkaUtils.PartitionConsumer(topic, partition, groupId)

    protected def doFetchRequest(fetchOffset: Long, fetchSize: Int): FetchResponse = {
      var tryFetchOffset = fetchOffset
      var numErrors = 0
      do {
        if (consumer == null) {
          consumer = new kafkaUtils.PartitionConsumer(topic, partition, groupId)
        }
        val fetchResponse = consumer.fetch(tryFetchOffset, fetchSize)

        if (fetchResponse.hasError) {
          numErrors += 1
          fetchResponse.errorCode(topic, partition) match {
            case code if (numErrors > 5) => throw new Exception("Error fetching data from leader,  Reason: " + code)
            case ErrorMapping.OffsetOutOfRangeCode => {
              tryFetchOffset = consumer.getLatestOffset
              println(s"readOffset ${topic}/${partition} out of range, resetting to latest offset ${tryFetchOffset}")
            }
            case code => {
              try {
                consumer.close
              } finally {
                consumer = null
              }
            }
          }
        } else {
          return fetchResponse
        }
      } while (numErrors > 0)
      throw new Exception("Error fetching data from leader, reason unknown")
    }

  }


}