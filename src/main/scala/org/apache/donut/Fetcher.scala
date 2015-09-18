package org.apache.donut

import java.util.concurrent.TimeUnit

import kafka.api.FetchResponse
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.message.MessageAndOffset
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 16/09/15.
 */
abstract class Fetcher(val task: DonutAppTask, topic: String, partition: Int, groupId: String) extends Runnable {

  private val log = LoggerFactory.getLogger(classOf[DonutAppTask])

  final protected val topicAndPartition = new TopicAndPartition(topic, partition)
  final protected var consumer = new task.kafkaUtils.PartitionConsumer(topic, partition, groupId)

  protected val initialOffset: Long = consumer.getEarliestOffset

  protected def onOutOfRangeOffset: Long

  protected def handleMessage(messageAndOffset: MessageAndOffset): Unit

  private[donut] def internalHandleMessage(messageAndOffset: MessageAndOffset): Unit
  //TODO private[donut] def handleMessageSet() instead of internalHandleMessage

  private[donut] var checkpointOffset = consumer.getOffset
  private var fetchOffset = initialOffset
  private var lastOffsetCommit = -1L
  private val offsetCommitIntervalNanos = TimeUnit.SECONDS.toNanos(10) // TODO configurable kafka.offset.commit.interval.ms

  override def run(): Unit = {
    try {
      log.debug(s"Start fetching ${topicAndPartition}")
      while (!Thread.interrupted) {
        //TODO this fetchSize might need to be controlled by config as it depends on compression and batch size of the producer
        val fetchResponse = doFetchRequest(fetchSize = 10 * 1024 * 1024)
        var numRead: Long = 0
        val messageSet = fetchResponse.messageSet(topic, partition)
        for (messageAndOffset <- messageSet) {
          fetchOffset = messageAndOffset.nextOffset
          internalHandleMessage(messageAndOffset)
          numRead += 1
        }
        if (fetchOffset >= checkpointOffset) {
          val nanoTime = System.nanoTime
          if (lastOffsetCommit + offsetCommitIntervalNanos < System.nanoTime) {
            consumer.commitOffset(fetchOffset)
            lastOffsetCommit = nanoTime
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
    } catch {
      //TODO propagate execption to the container so that it can exit with error code and inform AM
      case e: Throwable => e.printStackTrace()
    } finally {
      if (consumer != null) consumer.close
    }
  }

  final protected def doFetchRequest(fetchSize: Int): FetchResponse = {
    var numErrors = 0
    do {
      if (consumer == null) {
        consumer = new task.kafkaUtils.PartitionConsumer(topic, partition, groupId)
      }
      val fetchResponse = consumer.fetch(fetchOffset, fetchSize)

      if (fetchResponse.hasError) {
        numErrors += 1
        fetchResponse.errorCode(topic, partition) match {
          case code if (numErrors > 5) => throw new Exception("Error fetching data from leader,  Reason: " + code)
          case ErrorMapping.OffsetOutOfRangeCode => {
            fetchOffset = onOutOfRangeOffset
            println(s"readOffset ${topic}/${partition} out of range, resetting to offset ${fetchOffset}")
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

