package org.apache.donut

import java.util.concurrent.TimeUnit

import kafka.message.MessageAndOffset

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherRecursive (kafkaUtils: KafkaUtils, topic: String, partition: Int, groupId: String)
  extends Fetcher(kafkaUtils, topic, partition, groupId) {

  /**
   * processOffset - persistent offset mark for remembering up to which point was the stream processed
   */
  var processOffset: Long = consumer.getOffset

  /**
   * readOffset - ephemeral offset mark for keeping the LocalState
   */
  var readOffset: Long = consumer.getEarliestOffset

  var lastOffsetCommit = -1L
  val offsetCommitIntervalNanos = TimeUnit.SECONDS.toNanos(10) // TODO configurable kafka.offset.auto...

  def asyncProcessMessage(messageAndOffset: MessageAndOffset): Unit

  def asyncUpdateState(messageAndOffset: MessageAndOffset): Unit

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
}