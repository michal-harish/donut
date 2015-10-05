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

import scala.collection.JavaConverters._

/**
 * Created by mharis on 04/10/15.
 *
 * Not Thread-Safe
 */
class VarHashTable(val initialCapacityKb: Int, val loadFactor: Double = 0.7) {

  val cube = new util.HashMap[Int, GrowableHashTable]

  def sizeInBytes: Long = cube.values.asScala.map(_.sizeInBytes).sum

  def load: Double = cube.values.asScala.map(_.load).sum / cube.size

  def put(key: ByteBuffer, value: (Boolean, Short, Int)) = hashTable(key).put(key, value)

  def get(key: ByteBuffer): (Boolean, Short, Int) = hashTable(key).get(key)

  def flag(key: ByteBuffer, flagValue: Boolean) = hashTable(key).flag(key, flagValue)

  private def hashTable(key: ByteBuffer): GrowableHashTable = {
    val keyLen = key.remaining
    cube.get(keyLen) match {
      case null => synchronized {
        val hashTable = new GrowableHashTable(keyLen, initialCapacityKb, loadFactor)
        cube.put(keyLen, hashTable)
        hashTable
      }
      case hashTable => hashTable
    }
  }

  final class GrowableHashTable(val keyLen: Int, val initialCapacityKb: Int, val loadFactor: Double) {

    def sizeInBytes: Long = data.capacity

    private val INITIAL_CAPACITY = initialCapacityKb * 1024

    private var maxCollisions = 0

    private val rowLen = keyLen + 7 // 1 byte for inTransit flag, 2 bytes for segment, 4 bytes for memory pointer

    private var data: ByteBuffer = null

    private var size = 0 // number of positions

    private var loaded = 0 //number of used positions

    def load: Double = loaded.toDouble / size

    grow

    private def find(key: ByteBuffer): Int = {
      val hashCode = key.getInt(key.position)
      if (hashCode == 0) {
        throw new IllegalArgumentException
      }
      var hash = getHash(hashCode)
      var numCollisions = 0
      while (numCollisions <= maxCollisions) {
        val hashPos = hash * rowLen
        val inspectHashCode = data.getInt(hashPos)
        if (inspectHashCode == 0) {
          return -1
        } else if (inspectHashCode == hashCode && keyEquals(hashPos, key)) {
          return hashPos
        }
        hash = resolveCollision(hash)
        numCollisions += 1
      }
      -1
    }

    def get(key: ByteBuffer): (Boolean, Short, Int) = {
      find(key) match {
        case -1 => null
        case hashPos => {
          return (
            data.get(hashPos + keyLen) != 0,
            data.getShort(hashPos + keyLen + 1),
            data.getInt(hashPos + keyLen + 3)
            )
        }
      }
//      val hashCode = key.getInt(key.position)
//      if (hashCode == 0) {
//        throw new IllegalArgumentException
//      }
//      var hash = getHash(hashCode)
//      var numCollisions = 0
//      while (numCollisions <= maxCollisions) {
//        val hashPos = hash * rowLen
//        val inspectHashCode = data.getInt(hashPos)
//        if (inspectHashCode == 0) {
//          return null
//        } else if (inspectHashCode == hashCode && keyEquals(hashPos, key)) {
//          return (
//            data.get(hashPos + keyLen) != 0,
//            data.getShort(hashPos + keyLen + 1),
//            data.getInt(hashPos + keyLen + 3)
//            )
//        }
//        hash = resolveCollision(hash)
//        numCollisions += 1
//      }
//      null
    }

    def flag(key: ByteBuffer, flagValue: Boolean): Unit = {
      find(key) match {
        case -1 => throw new ArrayIndexOutOfBoundsException
        case hashPos => data.put(hashPos + keyLen, if (flagValue) 1 else 0)
      }
    }

    def put(key: ByteBuffer, value: (Boolean, Short, Int)): Unit = put(key, value, true)

    private def put(key: ByteBuffer, value: (Boolean, Short, Int), growable: Boolean): Boolean = {
      val hashCode = key.getInt(key.position)
      if (hashCode == 0) {
        throw new IllegalArgumentException
      }
      var hash = getHash(hashCode)
      var numCollisions = 0
      while (true) {
        val hashPos = hash * rowLen
        val inspectHashCode = data.getInt(hashPos)
        if (inspectHashCode == 0 || (inspectHashCode == hashCode && keyEquals(hashPos, key))) {
          var i = 0
          while (i < keyLen) {
            data.put(hashPos + i, key.get(key.position + i))
            i += 1
          }
          data.put(hashPos + keyLen, if (value._1) 1 else 0)
          data.putShort(hashPos + keyLen + 1, value._2)
          data.putInt(hashPos + keyLen + 3, value._3)
          loaded += 1
          return true
        }
        hash = resolveCollision(hash)
        numCollisions += 1
        if (numCollisions > maxCollisions) maxCollisions = numCollisions
        if (growable && load > loadFactor) {
          grow
          numCollisions = 0
          hash = getHash(hashCode)
        } else if (load >= 1.0) {
          throw new IllegalArgumentException(s"Could not resolve hash table collision, load ${load} % ")
        }
      }
      false
    }

    private def getHash(hashCode: Int): Int = {
      if (hashCode == Integer.MIN_VALUE) {
        Integer.MAX_VALUE % size
      } else {
        ((hashCode.toLong + Integer.MAX_VALUE) % size).toInt match {
          case h if (h >= size) => throw new IllegalArgumentException
          case h => h
        }
      }
    }

    private def resolveCollision(hash: Int): Int = {
      (hash + 1) % size
    }

    private def keyEquals(atPos: Int, other: ByteBuffer): Boolean = {
      var i = 0
      while (i < keyLen) {
        val l = data.get(atPos + i)
        val r = other.get(other.position + i)
        if (l != r) {
          return false
        }
        i += 1
      }
      true
    }

    private def grow = {
      val oldData = data
      var capacity = if (data == null) INITIAL_CAPACITY / 2 else oldData.capacity
      var success = true
      do {
        capacity = (capacity * 2).toInt / rowLen * rowLen
        data = ByteBuffer.allocateDirect(capacity)
        size = capacity / rowLen
        loaded = 0
        maxCollisions = 0
        if (oldData != null && oldData.capacity > 0) {
          var c = 0
          while (c < oldData.capacity) {
            if (oldData.getInt(c) != 0) {
              oldData.position(c)
              val flag = oldData.get(oldData.position + keyLen) != 0
              val segment = oldData.getShort(oldData.position + keyLen + 1)
              val pointer = oldData.getInt(oldData.position + keyLen + 3)
              success = success && put(oldData, (flag, segment, pointer), false)
            }
            c += rowLen
          }
        }
      } while (!success)
      true
    }

  }

}
