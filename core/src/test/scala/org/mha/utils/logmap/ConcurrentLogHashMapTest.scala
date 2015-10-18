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

package org.mha.utils.logmap

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}
import org.mha.utils.ByteUtils
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 02/10/15.
 */
class ConcurrentLogHashMapTest extends FlatSpec with Matchers {

  def g = (b: ByteBuffer) => {
    val a = new Array[Byte](b.remaining)
    b.get(a)
    new String(a)
  }

  behavior of "ConcurrentLogHashMap"

  it should "detect recycled block references (single-threaded) " in {
    val map = new ConcurrentLogHashMap(maxSizeInMb = 4, segmentSizeMb = 1, compressMinBlockSize = 16 * 1024)
    val words = List("Hello", "World", "Cambodia", "Japan", "Lebanon")
    def genKey(m: Long): ByteBuffer = {
      val a = new Array[Byte](12)
      ByteUtils.putLongValue(m, a, 4)
      ByteUtils.putIntValue(m.hashCode, a, 0)
      ByteBuffer.wrap(a)
    }
    def genVal(k: Long): ByteBuffer = {
      val shuffledWords = for (x <- 0 to 100) yield words(((k + x) % words.length).toInt)
      ByteBuffer.wrap(shuffledWords.mkString(",").getBytes)
    }
    def genSegment(kStart: Long): Unit = {
      map.put(genKey(kStart), genVal(kStart))
      val markSegment = map.currentSegment
      var k = kStart
      while (map.currentSegment == markSegment) {
        k += 1
        map.put(genKey(k), genVal(k))
      }
    }
    genSegment(10000000)
    map.stats(details = true).foreach(println)
    genSegment(20000000)
    map.stats(details = true).foreach(println)
    genSegment(10000000) //pushing out existing value from the last segment
    map.stats(details = true).foreach(println)
  }

  it should "behave consistently and perform (multi-threaded) " in {
    val m = new ConcurrentLogHashMap(maxSizeInMb = 64, segmentSizeMb = 16, compressMinBlockSize = 16 * 1024)
    val words = List("Hello", "World", "Foo", "Bar")
    val numWords = 100
    val stepFactor = 9997
    val counter = new AtomicInteger(0)
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
      throw new TimeoutException(s"${m.numSegments} SEGMENTS: count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, capacity = ${m.totalSizeInBytes / 1024} Kb")
    }

    println(s"input count ${counter.get}, ${time.get} ms")
    println(s"PUT ALL > ${m.numSegments} SEGMENTS: count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, capacity = ${m.totalSizeInBytes / 1024} Kb")
    m.size should be(counter.get)
    m.index.size should be(m.size)
    m.stats(details = true).foreach(println)
    m.compressRatio should be(1.0)

    getAll()
    getAll()
    println(s"COMPACT > ${m.numSegments} SEGMENTS: count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, capacity = ${m.totalSizeInBytes / 1024} Kb")
    m.totalSizeInBytes should be <= (m.maxSizeInBytes)
    m.compressRatio should be(1.0)
    getAll()

    //    m.applyCompression(0.75)
    //    println(s"COMPRESS > ${m.numSegments} SEGMENTS: count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, capacity = ${m.totalSizeInBytes / 1024} Kb")
    //    m.compressRatio should be < (0.7)
    //    getAll()
    //
    //    putMoreThanMaxAllowed
    //    println(s"EXTRA PUT ${counter.get}")
    //    println(s"PUT MORE> ${m.numSegments} SEGMENTS: count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, capacity = ${m.totalSizeInBytes / 1024} Kb")
    //    m.totalSizeInBytes should be <= (m.maxSizeInBytes)
    //    m.size should be < (counter.get)
    //    m.size should be > 70000
    //
    //    m.applyCompression(1.0)
    //
    //    def putMoreThanMaxAllowed = {
    //      val digest = MessageDigest.getInstance("MD5")
    //      val input = (((numThreads + 1) * stepFactor) to ((numThreads + 10) * stepFactor - 1)).map(i => (
    //        genKey(i, digest),
    //        ByteBuffer.wrap((0 to numWords).map(x => words(i % words.size)).mkString(",").getBytes))
    //      ).toMap
    //      counter.addAndGet(input.size)
    //      val ts = System.currentTimeMillis
    //      input.foreach { case (key, value) => {
    //        m.put(key, value)
    //      }
    //      }
    //      time.addAndGet(System.currentTimeMillis - ts)
    //    }

    def genKey(k: Int, digest: MessageDigest): ByteBuffer = {
      val d = digest.digest(k.toString.getBytes)
      ByteBuffer.wrap(d)
    }

    def getAll() = {
      val digest = MessageDigest.getInstance("MD5")
      for (t <- 1 to numThreads) {
        for (i <- (t * stepFactor) to ((t + 1) * stepFactor - 1)) {
          val key = genKey(i, digest)
          val expectedValue = (0 to numWords).map(x => words(i % words.size)).mkString(",")
          val actualValue = m.get(key, g)
          actualValue should be(expectedValue)
        }
      }
      println(s"GET ALL > ${m.numSegments} SEGMENTS: count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, capacity = ${m.totalSizeInBytes / 1024} Kb")
    }
  }

}
