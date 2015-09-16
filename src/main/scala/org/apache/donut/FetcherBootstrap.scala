package org.apache.donut

import kafka.message.MessageAndOffset
import scala.collection.JavaConverters._

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherBootstrap(task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

  private val bootSequenceId = s"$topic:$partition"

  task.bootSequence.put(bootSequenceId, false)

  val bootCheckpointOffset = consumer.getOffset

  override val initialOffset: Long = consumer.getEarliestOffset

  //FIXME sometimes the offset is not actually committed and the fetchers start where they have left off
  consumer.commitOffset(initialOffset)

  final override private[donut] def internalHandleMessage(messageAndOffset: MessageAndOffset): Unit = {
    handleMessage(messageAndOffset)

    if (!task.bootSequenceCompleted) {
      val bootSequenceId = s"$topic:$partition"
      if (!task.bootSequence.get(bootSequenceId) && messageAndOffset.offset >= bootCheckpointOffset) {
        task.bootSequence.put(bootSequenceId, true)
        println(s"${bootSequenceId} booted")
        if (task.bootSequence.values().asScala.forall(booted => booted)) {
          task.bootSequenceCompleted = true
          println(s"The boot sequence for logical partiton ${task.logicalPartition} completed!")
          task.bootSequence.synchronized {
            task.bootSequence.notifyAll
          }
        }
      }
    }
  }

}