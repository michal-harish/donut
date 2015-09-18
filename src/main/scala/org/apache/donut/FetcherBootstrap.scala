package org.apache.donut

import kafka.message.MessageAndOffset
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherBootstrap(task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

  private val log = LoggerFactory.getLogger(classOf[FetcherBootstrap])

  override protected val initialOffset: Long = consumer.getEarliestOffset

  override protected def onOutOfRangeOffset = consumer.getEarliestOffset

  private val bootSequenceId = s"$topic:$partition"

  private var booted = initialOffset >= checkpointOffset

  task.bootSequence.put(bootSequenceId, booted)

  log.debug(s"[$bootSequenceId]: initialOffset = $initialOffset, checkpoint offset = $checkpointOffset")

  final override private[donut] def internalHandleMessage(messageAndOffset: MessageAndOffset): Unit = {
    handleMessage(messageAndOffset)

    if (messageAndOffset.nextOffset >= checkpointOffset) {
      checkpointOffset = messageAndOffset.nextOffset
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

}