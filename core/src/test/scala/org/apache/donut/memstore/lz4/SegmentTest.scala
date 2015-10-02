package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

import org.scalatest.{Matchers, FlatSpec}

import scala.util.Random

/**
 * Created by mharis on 01/10/15.
 */

class SegmentTest extends FlatSpec with Matchers {

  val segment = new SegmentDirectMemory(capacityMb = 1)

  segment.compressRatio should be (0)

  println(s"segment.sizeInBytes = ${segment.sizeInBytes}, compression = ${segment.compressRatio} % ")

  val testArray = new Array[Byte](23000)
  val random = new Random
  random.nextBytes(testArray)
  for(i <- (0 to (testArray.length-1) / 3)) {
    testArray(i * 3) = 0
    testArray(i * 3 + 1) = 0
  }
  val testBlock = ByteBuffer.wrap(testArray)

  segment.put(testBlock) should be (0)

  val buffer = ByteBuffer.allocate(64 * 1024)
  segment.get(0, buffer).compareTo(testBlock) should be (0)
  segment.get(0, buffer).compareTo(testBlock) should be (0)

  println(s"put > segment.sizeInBytes = ${segment.sizeInBytes}, compression = ${segment.compressRatio} % ")

  segment.compressRatio should be > (30)

  segment.sizeInBytes should be < (25000)

  segment.put(testBlock, 0) should be (0)

  println(s"put > segment.sizeInBytes = ${segment.sizeInBytes}, compression = ${segment.compressRatio} % ")

  segment.sizeInBytes should be > (25000)

  segment.compressRatio should be > (30)

  segment.get(0, buffer).compareTo(testBlock) should be (0)

  segment.compact

  println(s"compact > segment.sizeInBytes = ${segment.sizeInBytes}, compression = ${segment.compressRatio} % ")

  segment.sizeInBytes should be < (25000)

  segment.compressRatio should be > (30)

  segment.remove(0)

  segment.get(0, buffer) should be (null)

  println(s"delete > segment.sizeInBytes = ${segment.sizeInBytes}, compression = ${segment.compressRatio} % ")

  segment.sizeInBytes should be > (10000)

  segment.compressRatio should be > (30)

  segment.compact

  println(s"compact> segment.sizeInBytes = ${segment.sizeInBytes}, compression = ${segment.compressRatio} % ")

  segment.sizeInBytes should be < (100)

  segment.compressRatio should be (0)


}
