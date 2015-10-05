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
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
 * Created by mharis on 01/10/15.
 */

class SegmentTest extends FlatSpec with Matchers {

  behavior of "SegmentDirectMemoryLZ4"

  it should "not regress in single-threaded context" in {
    val segment = new SegmentDirectMemoryLZ4(capacityMb = 1, compressMinBlockSize = 10240)

    segment.compressRatio should be(1.0)

    val testArray = new Array[Byte](23000)
    val random = new Random
    random.nextBytes(testArray)
    for (i <- (0 to (testArray.length - 1) / 3)) {
      testArray(i * 3) = 0
      testArray(i * 3 + 1) = 0
    }
    val testBlock = ByteBuffer.wrap(testArray)

    segment.put(testBlock) should be(0)

    segment.get(0).compareTo(testBlock) should be(0)
    segment.get(0).compareTo(testBlock) should be(0)

    segment.compressRatio should be < (0.7)

    segment.sizeInBytes should be < (25000)

    segment.put(testBlock, 0) should be(0)

    segment.sizeInBytes should be > (25000)

    segment.compressRatio should be < (0.7)

    segment.get(0).compareTo(testBlock) should be(0)

    segment.compact

    segment.sizeInBytes should be < (25000)

    segment.compressRatio should be < (0.7)

    segment.remove(0)

    an [ArrayIndexOutOfBoundsException] should be thrownBy(segment.get(0))

    segment.sizeInBytes should be (0)

    segment.compressRatio should be (1.0)

    segment.compact

    segment.sizeInBytes should be (0)

    segment.compressRatio should be(1.0)
  }

  it should "perform well in a multi-threaded context" in {
    val s = new SegmentDirectMemoryLZ4(capacityMb = 16, compressMinBlockSize = 512)

    val random = new Random
    val words = List("Hello", "World", "Foo", "Bar")
    //val ab = new Concurrent List of some kind to compare with
    println("Single-threaded init")
    for(i <- (1 to 25000)) {
      val value = ByteBuffer.wrap((0 to 100).map(x => words(math.abs(random.nextInt) % words.size)).mkString(",").getBytes)
      s.put(value)
    }
    println(s"size = ${s.sizeInBytes / 1024 / 1024} Mb, count = ${s.size}, compression = ${s.compressRatio} %")
    s.compact should be (false)
    s.sizeInBytes / 1024 / 1024 should be(10)
    s.compressRatio should be < (0.9)

    val numThreads = 4
    println(s"Multi-threaded parallel put & compact, numThreads = ${numThreads}")
    val processTime = System.currentTimeMillis
    val e = Executors.newFixedThreadPool(numThreads)
    for(t <- (1 to numThreads)) {
      e.submit(new Runnable() {
        override def run(): Unit = {
          try {
            for (i <- (0 to 25000-1)) {
              val value = ByteBuffer.wrap((0 to 100).map(x => words(math.abs(random.nextInt) % words.size)).mkString(",").getBytes)
              s.put(value, i)
              if (i % 10000 == 0) {
                s.compact
              }
            }
            s.compact
          } catch {
            case e: Throwable => {
              e.printStackTrace()
              System.exit(1)
            }
          }
        }
      })
    }
    e.shutdown
    if (!e.awaitTermination(10, TimeUnit.SECONDS)) {
      throw new TimeoutException(s"size = ${s.sizeInBytes / 1024 / 1024} Mb, count = ${s.size}, compression = ${s.compressRatio}")
    }
    println(s"parallel put & compact > size = ${s.sizeInBytes / 1024 / 1024} Mb, count = ${s.size}, compression = ${s.compressRatio}")
    println(s"parralel put & compact ${System.currentTimeMillis - processTime} ms")
    s.size should be (25000)
    s.compressRatio should be < (0.9)
    s.sizeInBytes / 1024 / 1024 should be (10)
  }

}
