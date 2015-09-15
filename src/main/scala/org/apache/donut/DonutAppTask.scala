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

  private val bootstrap = config.getBoolean("donut.bootstrap", false)

  private val partitionsToConsume: Map[String, Seq[Int]] = topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % totalLogicalPartitions == logicalPartition))
  }}

  final override def run: Unit = {
    println(s"Starting task for logical partition ${logicalPartition}/${totalLogicalPartitions}")
    val fetchers = partitionsToConsume.flatMap {
      case (topic, partitions) => partitions.map(partition => {
        new DonutFetcher(topic, partition, config.get("kafka.group.id"))
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
          val (readProgress, processProgress) = fetchers.map(_.progress).reduce((f1,f2) => (f1._1 + f2._1, f1._2 + f2._2))
          awaitingTermination(readProgress / fetchers.size, processProgress / fetchers.size)
        }
      }
    } finally {
      onShutdown
    }
  }

  def awaitingTermination(avgReadProgress: Float, avgProcessProgress: Float)


  def onShutdown

  def asyncUpdateState(messageAndOffset: kafka.message.MessageAndOffset)

  def asyncProcessMessage(messageAndOffset: kafka.message.MessageAndOffset)

  class DonutFetcher(topic: String, partition: Int, groupId: String) extends Runnable {
    val topicAndPartition = new TopicAndPartition(topic, partition)
    var consumer = new kafkaUtils.PartitionConsumer(topic, partition, groupId)

    /**
     * processOffset - persistent offset mark for remembering up to which point was the stream processed
     */
    var processOffset: Long = consumer.getOffset

    /**
     * readOffset - ephemeral offset mark for keeping the LocalState
     */
    var readOffset: Long = if (bootstrap) consumer.getEarliestOffset else processOffset

    var lastOffsetCommit = -1L
    val offsetCommitIntervalNanos = TimeUnit.SECONDS.toNanos(10) // TODO configurable kafka.offset.auto...

    def progress: (Float, Float) = {
      val currentEarliestOffset = consumer.getEarliestOffset
      val currentLatestOffset = consumer.getLatestOffset
      val availableOffsets = currentLatestOffset - currentEarliestOffset
      val processProgress = 100f * (processOffset - currentEarliestOffset) / availableOffsets
      val readProgress = 100f * (readOffset - currentEarliestOffset) / availableOffsets
      (readProgress, processProgress)
    }



    override def run(): Unit = {
      try {
        while (!Thread.interrupted) {
          //TODO this fetchSize of 100000 might need to be controlled by config if large batches are written to Kafka
          val fetchResponse = doFetchRequest(readOffset, fetchSize = 10000)
          var numRead: Long = 0
          val messageSet = fetchResponse.messageSet(topic, partition)
          for (messageAndOffset <- messageSet) {
            val currentOffset = messageAndOffset.offset
            if (currentOffset >= readOffset) {
              if (currentOffset >= processOffset) {
                asyncProcessMessage(messageAndOffset)
                processOffset = messageAndOffset.nextOffset
              }
              asyncUpdateState(messageAndOffset)
              readOffset = messageAndOffset.nextOffset
            }
            numRead += 1
          }
          val nanoTime = System.nanoTime
          if (lastOffsetCommit + offsetCommitIntervalNanos < System.nanoTime) {
            consumer.commitOffset(processOffset)
            lastOffsetCommit = nanoTime
          }

          if (numRead == 0) {
            try {
              Thread.sleep(1000) //Note: backoff sleep time
            } catch {
              case ie: InterruptedException => return
            }
          }
        }
      } catch {
        //TODO propagate execption to the container so that it can exit with error code and inform AM
        case e: Throwable => e.printStackTrace()
      } finally {
        if (consumer != null) consumer.close
      }
    }

    def doFetchRequest(fetchOffset: Long, fetchSize: Int): FetchResponse = {
      var numErrors = 0
      do {
        if (consumer == null) {
          consumer = new kafkaUtils.PartitionConsumer(topic, partition, groupId)
        }
        val fetchResponse = consumer.fetch(fetchOffset, fetchSize)

        if (fetchResponse.hasError) {
          numErrors += 1
          fetchResponse.errorCode(topic, partition) match {
            case code if (numErrors > 5) => throw new Exception("Error fetching data from leader,  Reason: " + code)
            case ErrorMapping.OffsetOutOfRangeCode => {
              println(s"readOffset ${topic}/${partition} out of range, resetting to earliest offset ")
              readOffset = consumer.getEarliestOffset
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