package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 04/10/15.
 */
class VarHashTableTest extends FlatSpec with Matchers {

  val digest = MessageDigest.getInstance("MD5")

  val range = (10000 to 99999)

  behavior of "java.util.HashMap"
  it should "be benchmark for VarLogHashMap" in {
    val h = new util.HashMap[ByteBuffer, (Boolean, Short, Int)]
    println(s"java.util.HashMap.put = ${put((key, sp) => h.put(key, sp), (key) => h.get(key))} ms")
    println(s"java.util.HashMap.get = ${get((key) => h.get(key))} ms")
  }

  behavior of "VarHashTable"

  it should "be comparable to HashMap" in {

    val h = new VarHashTable(initialCapacityKb = 4)

    val putMs = put((key, sp) => h.put(key, sp), (key) => h.get(key))
    println(s"VarHashTable.put = ${putMs}ms")
    println(s"VarHashTable.get = ${get((key) => h.get(key))} ms")
    println(s"VarHashTable ${h.sizeInBytes / 1024 / 1024} Mb, load ${h.load} %\n")

  }

  def get(f: (ByteBuffer) => (Boolean, Short, Int)): Long = {
    val ts = System.currentTimeMillis
    for (k <- range) {
      val key = genKey(k)
      f(key) should be((false, (k % 3 + 2).toShort, 2))
    }
    System.currentTimeMillis - ts
  }


  def put(f: (ByteBuffer, (Boolean, Short, Int)) => Unit, f2: (ByteBuffer) => (Boolean, Short, Int)): Long = {
    val ts = System.currentTimeMillis
    for (k <- range) {
      val key = genKey(k)
      f(key, (false, (k % 3 + 0).toShort, 0))
      f2(key) should be (false, (k % 3 + 0).toShort, 0)
      f(key, (false, (k % 3 + 1).toShort, 1))
      f2(key) should be (false, (k % 3 + 1).toShort, 1)
      f(key, (false, (k % 3 + 2).toShort, 2))
      f2(key) should be (false, (k % 3 + 2).toShort, 2)
    }
    System.currentTimeMillis - ts
  }

  def genKey(k: Int): ByteBuffer = {
    val d = digest.digest(k.toString.getBytes)
    //println(s"${k} -> hashCode = ${ByteUtils.asIntValue(d)}")
    ByteBuffer.wrap(d)
  }

}
