package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 02/10/15.
 */
class ConcurrentLogHashMapTest extends FlatSpec with Matchers {

  behavior of "ConcurrentLogHashMap"

  it should "be consistent and fast in a single-threaded context" in {
    val m = new ConcurrentLogHashMap(maxSegmentSizeMb = 1, compressMinBlockSize = 10240)
    val a = ByteBuffer.wrap("123456".getBytes)
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
    m.get(a)
    m.catalog.compact
    m.catalog.forceNewSegment
    m.get(a)
    m.catalog.forceNewSegment
    m.get(a)
    m.catalog.compact
    m.get(a)
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
        val digest = MessageDigest.getInstance("MD5")
        override def run(): Unit = {
          try {
            val input = ((t * stepFactor) to ((t + 1) * stepFactor - 1)).map(i => (
              genKey(i, digest),
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
    val digest = MessageDigest.getInstance("MD5")
    for (t <- 1 to numThreads) {
      for (i <- (t * stepFactor) to ((t + 1) * stepFactor - 1)) {
        val key = genKey(i,digest)
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

  def genKey(k: Int, digest: MessageDigest): ByteBuffer = {
    val d = digest.digest(k.toString.getBytes)
    ByteBuffer.wrap(d)
  }
}
