package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 02/10/15.
 */
class ConcurrentLogHashMapTest extends FlatSpec with Matchers {

  behavior of "ByteBufferLogHashMap"

  it should "be consistent and fast in a single-threaded context" in {
    val m = new ConcurrentLogHashMap(maxSegmentSizeMb = 1, compressMinBlockSize = 10240)
    val a = ByteBuffer.wrap("123456".getBytes)
//    println(s"start > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
//    println(s"put > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
//    println(s"put > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
//    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.catalog.compact
//    println(s"compact > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.catalog.forceNewSegment
//    println(s"new > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
//    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.catalog.forceNewSegment
//    println(s"new > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
//    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.catalog.compact
//    println(s"compact > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
//    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
  }

  it should "behave consistently and perform in a multi-threaded context" in {
    val m = new ConcurrentLogHashMap(maxSegmentSizeMb = 1, compressMinBlockSize = 1024)
    val words = List("Hello", "World", "Foo", "Bar")
    val numWords = 100
    val stepFactor = 9997
    val counter = new AtomicLong(0)
    val time = new AtomicLong(0)
    val numThreads = 4
    val e = Executors.newFixedThreadPool(numThreads)
    (1 to numThreads).foreach(t => {
      e.submit(new Runnable() {
        override def run(): Unit = {
          try {
            val input = ((t * stepFactor) to ((t + 1) * stepFactor - 1)).map(i => (
              ByteBuffer.wrap(i.toString.getBytes),
              ByteBuffer.wrap((0 to numWords).map(x => words(i % words.size)).mkString(",").getBytes))
            ).toMap
            counter.addAndGet(input.size)
            val ts = System.currentTimeMillis
            input.foreach { case (key, value) => {
              m.put(key, value)
            }
            }
            time.addAndGet(System.currentTimeMillis - ts)
          } catch {
            case e: Throwable => {
              e.printStackTrace
              System.exit(1)
            }
          }
        }
      })
    })
    e.shutdown
    if (!e.awaitTermination(10, TimeUnit.SECONDS)) {
      throw new TimeoutException(s"${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.count}, compression = ${m.compressRatio} %")
    }

    println(s"input count ${counter.get}, ${time.get} ms")
    println(s"PUT ALL > ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.count}, compression = ${m.compressRatio} %")
    m.count should be (counter.get)
    m.compressRatio should be (100)
    for (t <- 1 to numThreads) {
      for (i <- (t * stepFactor) to ((t + 1) * stepFactor - 1)) {
        val key = ByteBuffer.wrap(i.toString.getBytes)
        val expectedValue = (0 to numWords).map(x => words(i % words.size)).mkString(",")
        m.get(key, (b) => {
          val a = new Array[Byte](b.remaining)
          b.get(a)
          new String(a)
        }) should be (expectedValue)
      }
    }
    println(s"GET ALL > ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.count}, compression = ${m.compressRatio} %")
    m.compact
    println(s"COMPACT > ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.count}, compression = ${m.compressRatio} %")
  }
}
