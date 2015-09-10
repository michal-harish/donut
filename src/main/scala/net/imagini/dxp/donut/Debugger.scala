package net.imagini.dxp.donut

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, Executors}

import kafka.consumer.KafkaStream
import net.imagini.dxp.common.{Edge, Vid}
import org.apache.donut.{DonutProducer, DonutConsumer}

import scala.collection.mutable

/**
 * Created by mharis on 10/09/15.
 */
class Debugger(zooKeeper: String, producer: DonutProducer[GraphMessage],  numThreads: Int) {

  val counter = new AtomicLong(0)
  val tester = DonutConsumer
  val consumer = DonutConsumer(zooKeeper, "SyncsToGraphDebugger")
  val streams = consumer.createMessageStreams(Map("graphstream" -> numThreads))("graphstream")
  val executor = Executors.newFixedThreadPool(numThreads)

  def start: Unit = {
    for (stream <- streams) {
      executor.submit(new RecursiveProcessorThread(stream))
    }
  }

  def stop: Unit = {
    try {
      consumer.shutdown
    } finally {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        executor.shutdownNow
      }
    }
  }

  class RecursiveProcessorThread(stream: KafkaStream[Array[Byte], Array[Byte]]) extends Runnable {
    val MAX_ITER = 5
    val MAX_ENTRIES = 5000
    val state = new java.util.LinkedHashMap[Vid, mutable.Map[Vid, Edge]]() {
      override protected def removeEldestEntry(eldest: java.util.Map.Entry[Vid, mutable.Map[Vid, Edge]]): Boolean = {
        size() > MAX_ENTRIES;
      }
    }

    override def run(): Unit = {
      val it = stream.iterator
      while (it.hasNext) {
        val msgAndMeta = it.next
        val msgGraph = new GraphMessage(msgAndMeta.key, msgAndMeta.message)
        val vid = msgGraph.decodeKey
        val (iter, edges) = msgGraph.decodePayload
        counter.addAndGet(1)
        val existing = state.get(vid)
        if (existing == null) {
          state.put(vid, mutable.Map(edges.toSeq:_*))
        } else {
          val newEdges = edges.filter(n => !existing.contains(n._1))
          if (iter < MAX_ITER) {
            val nextIter = iter + 1
            producer.send(existing.map { case (e, props) => {
              GraphMessage(e, nextIter, newEdges)
            } }.toList)
          }
          existing ++= newEdges
        }
      }
    }
  }

}

