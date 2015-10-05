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
import java.util
import org.apache.donut.utils.ByteUtils
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 04/10/15.
 */
class VarHashTableTest extends FlatSpec with Matchers {

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
    println(s"VarHashTable ${h.sizeInBytes / 1024 / 1024} Mb, load ${h.load}\n")
    h.load should be <= 0.7

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
    val key = k.toString
    val b = new Array[Byte](key.length + 4)
    ByteUtils.putIntValue(key.hashCode, b, 0)
    ByteUtils.copy(key.getBytes, 0, b, 4, key.length)
    ByteBuffer.wrap(b)
  }

}
