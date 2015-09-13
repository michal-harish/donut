package org.apache.donut

import java.nio.ByteBuffer

import kafka.consumer.KafkaStream
import kafka.message.MessageAndMetadata

/**
 * Created by mharis on 13/09/15.
 */
abstract class RecursiveStateProcessor[I, S](
                                              maxEntries: Int,
                                              stream: KafkaStream[Array[Byte], Array[Byte]],
                                              t: (MessageAndMetadata[Array[Byte], Array[Byte]]) => I) extends Runnable {

  val localState = new LocalStorage[S](maxEntries)
  private var lastProcessedOffset = -1L

  final override def run: Unit = {
    val it = stream.iterator
    while (it.hasNext) {
      val msgAndMeta = it.next
      val msgOffset = msgAndMeta.offset
      val key = ByteBuffer.wrap(msgAndMeta.key)
      localState.get(key) match {
        case Some(null) => {
          //this key has been evicted and we remember it
        }
        case None => if (!initState(key, t(msgAndMeta))) {
          localState.put(key, null.asInstanceOf[S])
        }
        case Some(state) => {
          val i: I = t(msgAndMeta)
          try {
            if (msgOffset > lastProcessedOffset) {
              process(state, i)
              lastProcessedOffset = msgOffset
            }
          } finally {
            if (!updateState(key, state, i)) {
              localState.put(key, null.asInstanceOf[S])
            }
          }
        }
      }
    }
  }

  def process(state: S, input: I): Unit

  def initState(key: ByteBuffer, input: I): Boolean

  def updateState(key: ByteBuffer, state: S, input: I): Boolean

}
