package org.apache.donut

import kafka.message.{ByteBufferMessageSet, MessageAndOffset}
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherBootstrap(task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

  private val log = LoggerFactory.getLogger(classOf[FetcherBootstrap])

  override private[donut] def onOutOfRangeOffset = consumer.getEarliestOffset

  override private[donut] val initialFetchOffset: Long = consumer.getEarliestOffset

  private val bootSequenceId = s"$topic:$partition"

  private var booted = initialFetchOffset >= getCheckpointOffset

  task.bootSequence.put(bootSequenceId, booted)

  log.debug(s"[$bootSequenceId]: initialOffset = $initialFetchOffset, checkpoint offset = $getCheckpointOffset")

  final override private[donut]  def internalHandleMessageSet(messageSet: ByteBufferMessageSet): Long = {
    var nextFetchOffset:Long = -1L
    for(messageAndOffset <- messageSet) {
      handleMessage(messageAndOffset)
      nextFetchOffset = messageAndOffset.nextOffset
      if (nextFetchOffset >= getCheckpointOffset) {
        if (!booted) {
          booted = true
          task.bootSequence.synchronized {
            log.debug(s"[$bootSequenceId]: caught up with last state checkpoint")
            task.bootSequence.put(bootSequenceId, true)
            task.checkBootSequenceCompleted
          }
        }
      }
    }
    nextFetchOffset
  }

  protected def handleMessage(messageAndOffset: MessageAndOffset): Unit
}