package io.amient.utils.logmap

import java.nio.ByteBuffer
import java.util.concurrent.{Executors, TimeUnit}
import io.amient.utils.ByteUtils
import org.scalatest.Matchers

import scala.util.Random

/**
 * Created by mharis on 08/10/15.
 */
object LogMapInteractiveTest extends App with Matchers {

  val m = new ConcurrentLogHashMap(
    maxSizeInMb = 8, segmentSizeMb = 1, compressMinBlockSize = 65535, indexLoadFactor = 0.7)

  val numThreads = 4
  val e = Executors.newFixedThreadPool(numThreads)
  val start = System.currentTimeMillis
  val fraction = 10
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
            val t = System.currentTimeMillis
            val shuffledWords = for (x <- 0 to 10) yield words(((t + x) % words.length).toInt)
            m.put(genKey((t / fraction) + (r.nextGaussian * 1000).toLong), genVal(shuffledWords.mkString(",")))
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

  val in = scala.io.Source.stdin.getLines

  while (!e.isTerminated) {
    try {
      val c = in.next.split("\\s+").iterator
      c.next match {
        case "" => {
          println(s"Time units ellapsed: ${(System.currentTimeMillis - start) / fraction }")
        }
        //case "compress" => m.applyCompression(c.next.toDouble)
        case "exit" => {
          e.shutdownNow
          e.awaitTermination(3, TimeUnit.SECONDS)
        }
        case any => println("Usage:" +
          "\n\t[ENTER]\t\tprint basic stats" +
          "\n\tcompress <fraction>\t\tcompress any segments in the tail of the log that occupies more than <fraction> of total hash map memory" +
          "\n\texit")
      }
      m.stats(details = true).foreach(println)
    } catch {
      case i: InterruptedException => e.shutdownNow
      case t: Throwable => t.printStackTrace
    }
    println()
  }
}
