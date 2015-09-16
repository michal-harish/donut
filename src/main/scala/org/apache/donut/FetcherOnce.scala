package org.apache.donut

import kafka.message.MessageAndOffset

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherOnce(task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

  /**
   * processOffset - persistent offset mark for remembering up to which point was the stream processed
   */
  override val initialOffset: Long = consumer.getOffset

  final override private[donut]  def internalHandleMessage(messageAndOffset: MessageAndOffset): Unit = {
    if (!task.bootSequenceCompleted) task.bootSequence.synchronized {
      println(s"Fetcher ${topic}/${partition} waiting for state boot sequence to complete..")
      task.bootSequence.wait
      println(s"Resuming Fetcher ${topic}/${partition} after boot sequence")
    }
    handleMessage(messageAndOffset)
  }



}