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

import java.io.IOException
import java.util.concurrent.TimeUnit

import kafka.api.FetchResponse
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.message.{ByteBufferMessageSet, MessageAndOffset}
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 16/09/15.
 */
abstract class Fetcher(val task: DonutAppTask, topic: String, partition: Int, groupId: String) extends Runnable {

  final private val log = LoggerFactory.getLogger(classOf[DonutAppTask])

  final protected val topicAndPartition = new TopicAndPartition(topic, partition)

  final private val checkpointCommitIntervalNanos = TimeUnit.SECONDS.toNanos(10) // TODO configurable kafka.offset.commit.interval.ms

  final protected var consumer = new task.kafkaUtils.PartitionConsumer(topic, partition, groupId)

  private[donut] def fetchConsumerOffsetFromCoordinator(): Long = consumer.getOffset match {
    case invalidOffset: Long if (invalidOffset > consumer.getLatestOffset || invalidOffset < consumer.getEarliestOffset) => onOutOfRangeOffset()
    case validOffset: Long => validOffset
  }

  private[donut] def onOutOfRangeOffset(): Long

  /**
   * @param messageAndOffset
   * @return nextOffset of the message last processed
   */
  private[donut] def internalHandleMessageSet(messageAndOffset: ByteBufferMessageSet): Long

  private[donut] val initialFetchOffset: Long

  private var lastCheckpointCommitValue = fetchConsumerOffsetFromCoordinator()
  private var lastCheckpointCommitTime = -1L
  private[donut] def getCheckpointOffset = lastCheckpointCommitValue
  private var nextFetchOffset: Long = -1L

  override def run(): Unit = {
    try {
      nextFetchOffset = initialFetchOffset
      while (!Thread.interrupted) {
        //TODO this fetchSize might need to be controlled by config as it depends on compression and batch size of the producer
        val fetchResponse = doFetchRequest(fetchSize = 2 * 1024 * 1024)
        val messageSet = fetchResponse.messageSet(topic, partition)
        if (messageSet.isEmpty) {
          try {
            Thread.sleep(1000) //Note: backoff sleep time
          } catch {
            case ie: InterruptedException => return
          }
        } else {
          /**
           * This is where at-least-once guarantee is implied. E.g. if the implementing processor handled only a
           * portion of the message set above and then failed with exception, the checkpoint offset below will not be
           * reached and on the task re-start that portion will be will re-processed again.
           */
          internalHandleMessageSet(messageSet) match {
            case -1L => throw new IllegalStateException
            case nextFetchOffsetHandled => {
              nextFetchOffset = nextFetchOffsetHandled
              if (nextFetchOffsetHandled > lastCheckpointCommitValue) {
                val nanoTime = System.nanoTime
                if (lastCheckpointCommitTime + checkpointCommitIntervalNanos < System.nanoTime) {
                  try {
                    log.debug(s"Committing offset for ${topicAndPartition} in group {$groupId} to ${nextFetchOffset}, num message = ${nextFetchOffsetHandled - lastCheckpointCommitValue}")
                    consumer.commitOffset(nextFetchOffsetHandled)
                    lastCheckpointCommitTime = nanoTime
                    lastCheckpointCommitValue = nextFetchOffsetHandled
                  } catch {
                    case e: IOException => {
                      //TODO implement retries
                      throw e
                    }
                  }
                }
              }
            }
          }
        }
      }
    } catch {
      //TODO propagate execption to the container so that it can exit with error code and inform AM
      case e: Throwable => log.error("Terminating fetcher", e)
    } finally {
      if (consumer != null) consumer.close
    }
  }

  final protected def doFetchRequest(fetchSize: Int): FetchResponse = {
    var numErrors = 0
    do {
      if (consumer == null) {
        consumer = new task.kafkaUtils.PartitionConsumer(topic, partition, groupId)
      }

      val fetchResponse = consumer.fetch(nextFetchOffset, fetchSize)

      if (fetchResponse.hasError) {
        numErrors += 1
        fetchResponse.errorCode(topic, partition) match {
          case code if (numErrors > 5) => throw new Exception("Error fetching data from leader,  Reason: " + code)
          case ErrorMapping.OffsetOutOfRangeCode => {
            nextFetchOffset = onOutOfRangeOffset()
            log.warn(s"readOffset ${topic}/${partition} out of range, resetting to offset ${nextFetchOffset}")
          }
          case code => {
            try {
              log.warn(s"Closing consumer due to kafka fetch error code ${code}")
              consumer.close
            } finally {
              consumer = null
            }
          }
        }
      } else {
        return fetchResponse
      }
    } while (numErrors > 0)
    throw new Exception("Error fetching data from leader, reason unknown")
  }
}

