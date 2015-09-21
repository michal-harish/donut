package org.apache.donut

import kafka.message.{ByteBufferMessageSet, MessageAndOffset}
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherDelta(task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

  private val log = LoggerFactory.getLogger(classOf[FetcherDelta])

  override private[donut] val initialFetchOffset: Long = fetchConsumerOffsetFromCoordinator()

  override private[donut] def onOutOfRangeOffset = consumer.getEarliestOffset

  log.debug(s"${topicAndPartition}: initialOffset = ${initialFetchOffset} ")

  final override private[donut] def internalHandleMessageSet(messageSet: ByteBufferMessageSet): Long = {
    var nextFetchOffset:Long = -1L
    for(messageAndOffset <- messageSet) {
      if (!task.checkBootSequenceCompleted) task.bootSequence.synchronized {
        log.info(s"Fetcher ${topic}/${partition} waiting for state boot sequence to complete..")
        task.bootSequence.wait
        log.info(s"Resuming Fetcher ${topic}/${partition} after boot sequence")
      }
      if (messageAndOffset.offset >= getCheckpointOffset) {
        handleMessage(messageAndOffset)
      }
      if ( messageAndOffset.nextOffset < nextFetchOffset) throw new IllegalStateException()
      nextFetchOffset = messageAndOffset.nextOffset
    }
    nextFetchOffset
  }

  protected def handleMessage(messageAndOffset: MessageAndOffset): Unit

}