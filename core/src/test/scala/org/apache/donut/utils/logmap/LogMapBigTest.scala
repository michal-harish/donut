package org.apache.donut.utils.logmap

import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import org.apache.donut.utils.ByteUtils
import org.scalatest.Matchers

import scala.util.Random

/**
 * Created by mharis on 08/10/15.
 */
object LogMapBigTest extends App with Matchers {

  val m = new ConcurrentLogHashMap(maxSizeInMb = 32, segmentSizeMb = 4,
    compressMinBlockSize = 65535, indexLoadFactor = 0.7)

  val numThreads = 4
  val numPuts = new AtomicLong(0)
  val e = Executors.newFixedThreadPool(4)
  for (t <- (1 to numThreads)) {
    e.submit(new Runnable() {
      val r = new Random
      val words = List("Hello", "World", "Cambodia", "Japan", "Lebanon")

      def genKey(m: Int): ByteBuffer = {
        val a = new Array[Byte](8)
        ByteUtils.putIntValue(m, a, 0)
        ByteUtils.putIntValue(m, a, 4)
        ByteBuffer.wrap(a)
      }

      def genVal(v: String): ByteBuffer = ByteBuffer.wrap(v.getBytes)

      override def run(): Unit = {
        while (true) {
          val k = (System.currentTimeMillis / 1000).toInt //+ (r.nextGaussian * 60).toInt
          val word = words(k % words.length)
          m.put(genKey(k), genVal(word))
          numPuts.incrementAndGet
        }
      }
    })
  }

  while (!e.isTerminated) {
    Thread.sleep(3000)
    println(s"${numPuts.get }====================================================================================================")
    m.printStats
  }
}
