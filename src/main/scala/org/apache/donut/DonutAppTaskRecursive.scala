package org.apache.donut

import java.util.concurrent.TimeUnit

import kafka.api.FetchResponse
import kafka.common.{ErrorMapping, TopicAndPartition}
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 15/09/15.
 */
abstract class DonutAppTaskRecursive (config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  protected def asyncUpdateState(messageAndOffset: kafka.message.MessageAndOffset)

  final override protected def createFetcher(topic: String, partition: Int, groupId: String): Runnable = {
    new RecursiveFetcher(topic, partition, groupId)
  }

  class RecursiveFetcher(topic: String, partition: Int, groupId: String) extends Fetcher(topic, partition, groupId) {

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
}
