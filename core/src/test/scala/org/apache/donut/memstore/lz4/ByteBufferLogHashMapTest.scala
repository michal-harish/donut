package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by mharis on 02/10/15.
 */
class ByteBufferLogHashMapTest extends FlatSpec with Matchers {

  behavior of "LogHashMap"

  val m = new ByteBufferLogHashMap

  it should "blah" in {
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
}
