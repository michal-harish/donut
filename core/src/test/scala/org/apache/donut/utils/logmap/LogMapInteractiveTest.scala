package org.apache.donut.utils.logmap

import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import org.apache.donut.utils.ByteUtils
import java.util.concurrent.TimeUnit
import org.scalatest.Matchers

import scala.util.Random

/**
 * Created by mharis on 08/10/15.
 */
object LogMapInteractiveTest extends App with Matchers {

  val m = new ConcurrentLogHashMap(
    maxSizeInMb = 32, segmentSizeMb = 2, compressMinBlockSize = 65535, indexLoadFactor = 0.7)

  val numThreads = 8
  val numPuts = new AtomicLong(0)
  val e = Executors.newFixedThreadPool(numThreads)
  val start = System.currentTimeMillis
  val fraction = 1
  for (t <- (1 to numThreads)) {
    e.submit(new Runnable() {
      val r = new Random
      val words = List("Hello", "World", "Cambodia", "Japan", "Lebanon")

      def genKey(m: Long): ByteBuffer = {
        val a = new Array[Byte](12)
        ByteUtils.putLongValue(m, a, 4)
        ByteUtils.putIntValue(m.hashCode, a, 0)
        ByteBuffer.wrap(a)
      }

      def genVal(v: String): ByteBuffer = ByteBuffer.wrap(v.getBytes)

      override def run(): Unit = {
        try {
          while (true) {
            val k = System.currentTimeMillis / fraction
            val word = words((k % words.length).toInt)
            m.put(genKey(k), genVal(word))
            numPuts.incrementAndGet
          }
        } catch {
          case e: InterruptedException => return
          case e: Throwable => {
            e.printStackTrace()
          }
        }
      }
    })
  }

  while (!e.isTerminated) {
    val in = scala.io.Source.stdin.getLines
    while (in.hasNext) {
      val c = in.next.split("\\s+").iterator
      c.next match {
        case "" => println("Expected size cca " + ((System.currentTimeMillis - start) / fraction))
        case "compact" => m.compact
        case "compress" => m.applyCompression(c.next.toDouble)
        case "exit" => {
          e.shutdownNow
          e.awaitTermination(3, TimeUnit.SECONDS)
        }
      }
      m.printStats
      println()
    }
  }
}
