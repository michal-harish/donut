package net.imagini.dxp.donut

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import net.imagini.dxp.common.{Vid, Edge}
import org.apache.donut.{LocalStorage, DonutProducer, DonutConsumer}

import scala.collection.mutable

/**
 * Created by mharis on 10/09/15.
 */
class RecursiveProcessorHighLevel(zooKeeper: String, producer: DonutProducer[GraphMessage]) extends Runnable {

  val MAX_ITER = 5
  val MAX_EDGES = 99

  val localState = new LocalStorage[mutable.Set[Vid]](500000)

  var lastProcessedOffset = -1L
  val counter1 = new AtomicLong(0)
  val counter2 = new AtomicLong(0)
  val counter3 = new AtomicLong(0)
  val counter4 = new AtomicLong(0)
  val tester = DonutConsumer
  val consumer = DonutConsumer(zooKeeper, "SyncsToGraphDebugger")
  val stream = consumer.createMessageStreams(Map("graphstream" -> 1))("graphstream").head

  override def run: Unit = {
    val it = stream.iterator
    while (it.hasNext) {
      val msgAndMeta = it.next
      val msgOffset = msgAndMeta.offset
      counter1.incrementAndGet
      val key = ByteBuffer.wrap(msgAndMeta.key)
      localState.get(key) match {
        case Some(null) => {
          counter4.incrementAndGet
          //this key has been evicted and we remember it
        }
        case None => {
          counter2.incrementAndGet
          val (iteration, inputEdges) = GraphMessage(msgAndMeta).decodePayload
          initState(key, inputEdges)
        }
        case Some(state) => {
          counter3.incrementAndGet
          val (iteration, inputEdges) = GraphMessage(msgAndMeta).decodePayload
          try {
            if (msgOffset > lastProcessedOffset) {
              process(state, iteration, inputEdges)
              lastProcessedOffset = msgOffset
            }
          } finally {
            updateState(key, state, inputEdges)
          }
        }
      }
    }
  }

  def process(state: mutable.Set[Vid], iteration: Int, inputEdges: Map[Vid, Edge]): Unit = {
    if (iteration < MAX_ITER) {
      val additionalEdges = if (state == null) inputEdges else inputEdges.filter(n => !state.contains(n._1))
      producer.send(state.map(e => GraphMessage(e, iteration + 1, additionalEdges)).toList)
    }
  }


  def initState(key: ByteBuffer, inputEdges: Map[Vid, Edge]): Unit = {
    if (inputEdges.size > MAX_EDGES) {
      localState.put(key, null)
    } else {
      localState.put(key, mutable.Set(inputEdges.map(_._1).toSeq: _*))
    }
  }

  def updateState(key: ByteBuffer, state: mutable.Set[Vid], inputEdges: Map[Vid, Edge]): Unit = {
    val newEdges = inputEdges.filter(n => !state.contains(n._1))
    if (newEdges.size + state.size > MAX_EDGES) {
      localState.put(key, null)
    } else {
      state ++= newEdges.map(_._1)
    }
  }
}

