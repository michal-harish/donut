package io.amient.donut

/**
 * Donut - Recursive Stream Processing Framework
 * Copyright (C) 2015 Michal Harish
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

  /**
   * This shouldn't be called too frequently as it queries the broker for the earliest and latest offsets
   * @return proportion of consumed to available data in the underlying kafka partition
   */
  override def getProgressRange: (Long, Long) = (consumer.getEarliestOffset, consumer.getLatestOffset)

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