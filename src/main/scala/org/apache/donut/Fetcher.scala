package org.apache.donut

import java.util.concurrent.TimeUnit

import kafka.api.FetchResponse
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.message.MessageAndOffset

/**
 * Created by mharis on 16/09/15.
 */
abstract class Fetcher(val kafkaUtils: KafkaUtils, topic: String, partition: Int, groupId: String) extends Runnable {
  final protected val topicAndPartition = new TopicAndPartition(topic, partition)
  final protected var consumer = new kafkaUtils.PartitionConsumer(topic, partition, groupId)
  private var lastOffsetCommit = -1L
  private val offsetCommitIntervalNanos = TimeUnit.SECONDS.toNanos(10) // TODO configurable kafka.offset.auto...

  protected val initialOffset: Long = consumer.getEarliestOffset
  def asyncProcessMessage(messageAndOffset: MessageAndOffset): Unit

  override def run(): Unit = {
    try {
      var processOffset = initialOffset
      while (!Thread.interrupted) {
        //TODO this fetchSize of 512Kb might need to be controlled by config if large batches are written to Kafka
        val fetchResponse = doFetchRequest(processOffset, fetchSize = 512 * 1024)
        var numRead: Long = 0
        val messageSet = fetchResponse.messageSet(topic, partition)
        for (messageAndOffset <- messageSet) {
          val currentOffset = messageAndOffset.offset
          if (currentOffset >= processOffset) {
            asyncProcessMessage(messageAndOffset)
            processOffset = messageAndOffset.nextOffset
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

  final protected def doFetchRequest(fetchOffset: Long, fetchSize: Int): FetchResponse = {
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

