package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 02/10/15.
 */
class ByteBufferLogHashMapTest extends FlatSpec with Matchers {

  behavior of "ByteBufferLogHashMap"

  it should "be consistent and fast in a single-threaded context" in {
    val m = new ByteBufferLogHashMap(1)
    val a = ByteBuffer.wrap("123456".getBytes)
    println(s"put > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
    println(s"put > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
    println(s"put > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.compact
    println(s"compact > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.createNewSegment
    println(s"new > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.createNewSegment
    println(s"new > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.compact
    println(s"compact > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
    m.get(a)
    println(s"get > ${m.numSegments} SEGMENTS: sizeInBytes = ${m.sizeInBytes}, count = ${m.count}, compression = ${m.compressRatio} %")
  }

//  it should "behave consistently and perform in a multi-threaded context" in {
//    val m = new ByteBufferLogHashMap(maxSegmentSizeMb = 2)
//    val random = new Random
//    val words = List("Hello", "World", "Foo", "Bar")
//    val counter = new AtomicLong(0)
//    val time = new AtomicLong(0)
//    val numThreads = 4
//    val e = Executors.newFixedThreadPool(numThreads)
//    (1 to numThreads).foreach(t => {
//      e.submit(new Runnable() {
//        override def run(): Unit = {
//          val input = ((t * 100000) to (t * 100000 + 10000)).map(i => (
//            ByteBuffer.wrap(ByteUtils.parseUUID(UUID.randomUUID.toString)),
//            ByteBuffer.wrap((0 to 10).map(x => words(math.abs(random.nextInt) % words.size)).mkString(",").getBytes))
//          ).toMap
//          counter.addAndGet(input.size)
//          val ts = System.currentTimeMillis
//          input.foreach{ case (key,value) => {
//            m.put(key, value)
//          }}
//          time.addAndGet(System.currentTimeMillis - ts)
//        }
//      })
//    })
//    e.shutdown
//    if (!e.awaitTermination(5, TimeUnit.SECONDS)) {
//      throw new IllegalStateException(s"${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.count}, compression = ${m.compressRatio} %")
//    }
//
//    println(counter.get)
//    println("================================================================")
//
//    println(s"input ${time.get} ms")
//    println(s"${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.count}, compression = ${m.compressRatio} %")
//  }
}
