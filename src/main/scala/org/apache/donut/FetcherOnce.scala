package org.apache.donut

import kafka.message.MessageAndOffset
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherOnce(task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

  private val log = LoggerFactory.getLogger(classOf[FetcherOnce])
  /**
   * processOffset - persistent offset mark for remembering up to which point was the stream processed
   */
  override val initialOffset: Long = checkpointOffset

  override protected def onOutOfRangeOffset = consumer.getEarliestOffset //TODO configurable out of range beahviour

  log.debug(s"${topicAndPartition}: initialOffset = ${initialOffset} ")

  final override private[donut]  def internalHandleMessage(messageAndOffset: MessageAndOffset): Unit = {
    if (!task.checkBootSequenceCompleted) task.bootSequence.synchronized {
        log.debug(s"Fetcher ${topic}/${partition} waiting for state boot sequence to complete..")
        task.bootSequence.wait
        log.debug(s"Resuming Fetcher ${topic}/${partition} after boot sequence")
    }
    if (messageAndOffset.offset >= checkpointOffset) {
      handleMessage(messageAndOffset)
      checkpointOffset = messageAndOffset.nextOffset
    }
  }

}