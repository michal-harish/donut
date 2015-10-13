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

import net.jpountz.lz4.LZ4Factory
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
 * Created by mharis on 01/10/15.
 */

class SegmentTest extends FlatSpec with Matchers {

//  behavior of "LZ4 library"
//  it should "be able to compress over the source byte buffer" in {
//    val str = (0 to 10).map(_ => "The quick brown fox jumps over the lazy dog").mkString(".")
//    val b = ByteBuffer.wrap(str.getBytes)
//    println(s"source: $b}")
//    val c = ByteBuffer.allocate(str.length)
//    val compressor = LZ4Factory.fastestInstance.highCompressor
//    println(s"dest: $c, max = ${compressor.maxCompressedLength(str.length)}}")
//    compressor.compress(b, c)
//    c.flip
//    println(s"compressed: $c}")
//    new String(c.array) should not be (str)
//    val decompressor = LZ4Factory.fastestInstance.fastDecompressor
//    val d = ByteBuffer.allocate(str.length)
//    decompressor.decompress(c, d)
//    new String(d.array) should be(str)
//  }

  behavior of "SegmentDirectMemoryLZ4"

  it should "be consistent in a single-threaded context" in {
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

    segment.put(-1, testBlock) should be(0)

    segment.get(0).compareTo(testBlock) should be(0)
    segment.get(0).compareTo(testBlock) should be(0)

    segment.compressRatio should be (1.0)

    segment.usedBytes should be < (40000)

    segment.put(0, testBlock) should be(0)

    segment.usedBytes should be < (40000)

    segment.compressRatio should be (1.0)

    segment.get(0).compareTo(testBlock) should be(0)

    segment.compact(0) should be(false)

    segment.usedBytes should be < (40000)

    segment.compressRatio should be (1.0)

    segment.remove(0)

    an[ArrayIndexOutOfBoundsException] should be thrownBy (segment.get(0))

    segment.usedBytes should be(0)
  }

  it should "group multiple blocks into a single lz4block of min size" in {
    println("Segment LZ4 Compression ...")
    val segment = new SegmentDirectMemoryLZ4(capacityMb = 1, compressMinBlockSize = 65535 * 2)

    segment.compressRatio should be(1.0)

    val testArray = new Array[Byte](10485 - 9)
    val random = new Random
    random.nextBytes(testArray)
    for (i <- (0 to (testArray.length - 1) / 3)) {
      testArray(i * 3) = 0
      testArray(i * 3 + 1) = 0
    }
    val testBlock = ByteBuffer.wrap(testArray)


    val numEntries = 100
    for (p <- (0 to numEntries - 1)) {
      segment.put(-1, testBlock) should be(p)
    }
    println(s"${numEntries} x PUT > s.size = ${segment.size}, s.compression = ${segment.compressRatio}; ${segment.usedBytes / 1024} Kb")
    segment.compressRatio should be(1.0)

    //no more entries can fit
    segment.put(-1, testBlock) should be(-1)

    for (p <- (0 to numEntries - 1)) {
      segment.get(p) should be(testBlock)
    }
    println(s"${numEntries} x GET > s.size = ${segment.size}, s.compression = ${segment.compressRatio}; ${segment.usedBytes / 1024} Kb")
    segment.printStats(0)
    segment.compress
    println(s"COMPRESS > s.size = ${segment.size}, s.compression = ${segment.compressRatio}; ${segment.usedBytes / 1024} Kb")

    segment.compressRatio should be < (0.2)
    for (p <- (0 to numEntries - 1)) {
      segment.get(p) should be(testBlock)
    }

    //after compression we should be able to fit more blocks in
    for (p <- (0 to numEntries / 2 - 1)) {
      segment.put(-1, testBlock) should be(numEntries + p)
    }
    println(s"${numEntries / 2} x PUT > s.size = ${segment.size}, s.compression = ${segment.compressRatio}; ${segment.usedBytes / 1024} Kb")

    for (p <- (0 to numEntries + (numEntries / 2) - 1)) {
      segment.get(p) should be(testBlock)
    }
    println(s"${1.5 * numEntries} x GET > s.size = ${segment.size}, s.compression = ${segment.compressRatio}; ${segment.usedBytes / 1024} Kb")

  }

  it should "perform well in a multi-threaded context" in {
    val s = new SegmentDirectMemoryLZ4(capacityMb = 16, compressMinBlockSize = 512)

    val random = new Random
    val words = List("Hello", "World", "Foo", "Bar")
    //val ab = new Concurrent List of some kind to compare with
    println("Single-threaded init")
    for(i <- (1 to 25000)) {
      val value = ByteBuffer.wrap((0 to 100).map(x => words(math.abs(random.nextInt) % words.size)).mkString(",").getBytes)
      s.put(-1, value)
    }
    println(s"size = ${s.usedBytes / 1024 / 1024} Mb, count = ${s.size}, compression = ${s.compressRatio} %")
    s.compact(0) should be (false)
    s.usedBytes / 1024 / 1024 should be(12)
    s.compressRatio should be (1.0)

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
              s.put(i, value)
              if (i % 10000 == 0) {
                s.compact(0)
              }
            }
            s.compact(0)
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
      throw new TimeoutException(s"size = ${s.usedBytes / 1024 / 1024} Mb, count = ${s.size}, compression = ${s.compressRatio}")
    }
    println(s"parallel put & compact > size = ${s.usedBytes / 1024 / 1024} Mb, count = ${s.size}, compression = ${s.compressRatio}")
    println(s"parralel put & compact ${System.currentTimeMillis - processTime} ms")
    s.size should be (25000)
    s.compressRatio should be (1.0)
    s.usedBytes / 1024 / 1024 should be (12)
  }

}
