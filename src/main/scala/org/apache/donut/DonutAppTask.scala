package org.apache.donut

import java.util.concurrent.{TimeUnit, Executors}

import kafka.api.{FetchResponse, FetchRequestBuilder}
import kafka.common.ErrorMapping
import kafka.consumer.SimpleConsumer
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

  private val kafkaUtils = KafkaUtils(config)

  private val topicPartitions: Map[String, Int] = kafkaUtils.getPartitionMap(topics)

  private val partitionsToConsume: Map[String, Seq[Int]] = topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % totalLogicalPartitions == logicalPartition))
  }
  }

  private val executor = Executors.newFixedThreadPool(partitionsToConsume.map(_._2.size).sum)

  final override def run: Unit = {
    println(s"Starting task for logical partition ${logicalPartition}/${totalLogicalPartitions}")
    partitionsToConsume.foreach {
      case (topic, partitions) => partitions.foreach(partition => {
        executor.submit(new DonutFetcher(topic, partition))
      })
    }
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

  def awaitingTermination

  def onShutdown

  def asyncProcessMessage(messageAndOffset: kafka.message.MessageAndOffset)

  class DonutFetcher(topic: String, partition: Int) extends Runnable {
    val clientName = "DonutFetcher_" + topic + "_" + partition
    var readOffset: Long = -1
    var consumer: SimpleConsumer = null

    override def run(): Unit = {
      try {
        while (!Thread.interrupted) {
          val fetchResponse = doFetchRequest
          var numRead: Long = 0
          for (messageAndOffset <- fetchResponse.messageSet(topic, partition)) {
            val currentOffset = messageAndOffset.offset
            if (currentOffset >= readOffset) {
              readOffset = messageAndOffset.nextOffset
              asyncProcessMessage(messageAndOffset)
              numRead += 1
            }
          }
          if (numRead == 0) {
            try {
              Thread.sleep(1000) //Note: backoff sleep time
            } catch {
              case ie: InterruptedException => return
            }
          }
        }
      } finally {
        if (consumer != null) consumer.close
      }
    }

    def doFetchRequest: FetchResponse = {
      var numErrors = 0
      do {
        if (consumer == null) {
          val leadBroker = kafkaUtils.findLeader(topic, partition)
          consumer = new SimpleConsumer(leadBroker, kafkaUtils.port, kafkaUtils.soTimeout, kafkaUtils.bufferSize, clientName)
          readOffset = kafkaUtils.getEarliestOffset(consumer, topic, partition, clientName)
          println(s"${clientName}, read from offset = ${readOffset}")
        }
        val req = new FetchRequestBuilder()
          .clientId(clientName)
          .addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
          .build()
        val fetchResponse = consumer.fetch(req);

        if (fetchResponse.hasError) {
          numErrors += 1
          fetchResponse.errorCode(topic, partition) match {
            case code if (numErrors > 5) => throw new Exception("Error fetching data from leader,  Reason: " + code)
            case ErrorMapping.OffsetOutOfRangeCode => {
              readOffset = kafkaUtils.getEarliestOffset(consumer, topic, partition, clientName);
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