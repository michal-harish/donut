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

package org.apache.donut.utils.logmap

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}
import org.apache.donut.utils.ByteUtils
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 02/10/15.
 */
class ConcurrentLogHashMapTest extends FlatSpec with Matchers {

  behavior of "ConcurrentLogHashMap"

  it should "be consistent and fast in a single-threaded context" in {
    val m = new ConcurrentLogHashMap(maxSizeInMb = 10, segmentSizeMb = 1, compressMinBlockSize = 4096)
    val a = ByteBuffer.wrap("123456".getBytes)
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
    m.put(a, ByteBuffer.wrap("Hello".getBytes))
    m.get(a)
    m.catalog.compact
    m.catalog.createNewSegment
    m.get(a)
    m.catalog.createNewSegment
    m.get(a)
    m.catalog.compact
    m.get(a)
  }

  it should "behave consistently and perform in a multi-threaded context" in {
    val m = new ConcurrentLogHashMap(maxSizeInMb = 64, segmentSizeMb = 16, compressMinBlockSize = 4096)
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
      throw new TimeoutException(s"${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, free = ${m.freeBytes / 1024} Kb")
    }

    println(s"input count ${counter.get}, ${time.get} ms")
    println(s"PUT ALL > ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, free = ${m.freeBytes / 1024} Kb")
    m.size should be (counter.get)
    m.compressRatio should be (1.0)

    getAll
    getAll
    m.compact
    println(s"COMPACT > ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, free = ${m.freeBytes / 1024} Kb")
    m.sizeInBytes should be <= (m.maxSizeInBytes)

    putMoreThanMaxAllowed
    println(s"EXTRA PUT ${counter.get}")
    println(s"PUT MORE> ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, free = ${m.freeBytes / 1024} Kb")
    m.sizeInBytes should be <= (m.maxSizeInBytes)
    m.size should be < (counter.get)

    def putMoreThanMaxAllowed = {
      val digest = MessageDigest.getInstance("MD5")
      val input = (((numThreads + 1) * stepFactor) to ((numThreads + 10) * stepFactor - 1)).map(i => (
        genKey(i, digest),
        ByteBuffer.wrap((0 to numWords).map(x => words(i % words.size)).mkString(",").getBytes))
      ).toMap
      counter.addAndGet(input.size)
      val ts = System.currentTimeMillis
      input.foreach { case (key, value) => {
        if (ByteUtils.asIntValue(key.array) == Int.MinValue) {
          println("!!")
        }
        m.put(key, value)
      }
      }
      time.addAndGet(System.currentTimeMillis - ts)
    }

    def getAll = {
      val digest = MessageDigest.getInstance("MD5")
      for (t <- 1 to numThreads) {
        for (i <- (t * stepFactor) to ((t + 1) * stepFactor - 1)) {
          val key = genKey(i, digest)
          val expectedValue = (0 to numWords).map(x => words(i % words.size)).mkString(",")
          m.get(key, (b) => {
            val a = new Array[Byte](b.remaining)
            b.get(a)
            new String(a)
          }) should be(expectedValue)
        }
      }
      println(s"GET ALL > ${m.numSegments} SEGMENTS: size = ${m.sizeInBytes / 1024} Kb, count = ${m.size}, compression = ${m.compressRatio}, load = ${m.load}, free = ${m.freeBytes / 1024} Kb")
    }
  }

  def genKey(k: Int, digest: MessageDigest): ByteBuffer = {
    val d = digest.digest(k.toString.getBytes)
    ByteBuffer.wrap(d)
  }
}
