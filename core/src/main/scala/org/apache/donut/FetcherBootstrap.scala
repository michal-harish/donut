package org.apache.donut

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