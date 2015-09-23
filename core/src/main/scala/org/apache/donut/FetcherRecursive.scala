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

import java.util.concurrent.TimeUnit

import kafka.message.MessageAndOffset

/**
 * Created by mharis on 16/09/15.
 */
@deprecated
abstract class FetcherRecursive (task: DonutAppTask, topic: String, partition: Int, groupId: String)
  extends Fetcher(task, topic, partition, groupId) {

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

  def internalHandleMessage(messageAndOffset: MessageAndOffset): Unit

  def asyncUpdateState(messageAndOffset: MessageAndOffset): Unit

  override def run(): Unit = {
    try {
      while (!Thread.interrupted) {
        //TODO this fetchSize of 100000 might need to be controlled by config if large batches are written to Kafka
        val fetchResponse = doFetchRequest(fetchSize = 10000)
        var numRead: Long = 0
        val messageSet = fetchResponse.messageSet(topic, partition)
        for (messageAndOffset <- messageSet) {
          val currentOffset = messageAndOffset.offset
          if (currentOffset >= readOffset) {
            if (currentOffset >= processOffset) {
              internalHandleMessage(messageAndOffset)
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