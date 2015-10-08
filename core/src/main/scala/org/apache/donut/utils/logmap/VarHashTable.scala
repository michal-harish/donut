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
 *
 */
class VarHashTable(val initialCapacityKb: Int, loadFactor: Double = 0.7) {

  //TODO class VarHashTable[K] and use K.hashCode so that we can do correction for 0 and Int.MinValue hashCodes transparently

  type VAL = (Boolean, Short, Int)

  val cube = new util.HashMap[Int, GrowableHashTable]

  def size: Long = cube.values.asScala.map(_.size).sum

  def sizeInBytes: Long = cube.values.asScala.map(_.sizeInBytes).sum

  def load: Double = cube.values.asScala.map(_.load).sum / cube.size

  def put(key: ByteBuffer, value: VAL) = hashTable(key, create = true).put(key, value)

  def get(key: ByteBuffer): VAL = hashTable(key).get(key)

  def contains(key: ByteBuffer): Boolean = hashTable(key).find(key) != -1

  def remove(key: ByteBuffer): Unit = hashTable(key).remove(key)

  def flag(key: ByteBuffer, flagValue: Boolean) = hashTable(key).flag(key, flagValue)

  def update(f: (VAL) => VAL): Unit = {
    cube.values.asScala.foreach(hashTable => hashTable.update(f))
  }

  def iterator = new Iterator[(ByteBuffer, VAL)] {
    private val ct = cube.values.iterator
    private var tt: Iterator[(ByteBuffer, VAL)] = null

    override def next(): (ByteBuffer, (Boolean, Short, Int)) = tt.next

    override def hasNext: Boolean = {
      if (tt == null || !tt.hasNext) {
        if (ct.hasNext) {
          tt = ct.next.iterator
        } else {
          return false
        }
      }
      tt.hasNext
    }
  }

  private def hashTable(key: ByteBuffer, create: Boolean = false): GrowableHashTable = {
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

    def size = loaded

    def load: Double = loaded.toDouble / numPositions

    private val INITIAL_CAPACITY = initialCapacityKb * 1024

    private var maxCollisions = 0

    private val rowLen = keyLen + 7 // 1 byte for inTransit flag, 2 bytes for segment, 4 bytes for memory pointer

    private var data: ByteBuffer = null

    private var numPositions = 0 // number of positions

    private var loaded = 0 //number of used positions

    grow

    def get(key: ByteBuffer): VAL = {
      find(key) match {
        case -1 => null
        case hashPos => return getValue(hashPos)
      }
    }

    def flag(key: ByteBuffer, flagValue: Boolean): Unit = {
      find(key) match {
        case -1 => throw new ArrayIndexOutOfBoundsException
        case hashPos => data.put(hashPos + keyLen, if (flagValue) 1 else 0)
      }
    }

    def remove(key: ByteBuffer): Unit = {
      find(key) match {
        case -1 => throw new ArrayIndexOutOfBoundsException(new String(key.array, 4, key.remaining - 4) + s" hashCode = ${key.getInt(0)}")
        case hashPos => {
          data.putInt(hashPos, Int.MinValue)
          loaded -= 1
          if (loaded == 0) maxCollisions = 0
        }
      }
    }

    def put(key: ByteBuffer, value: VAL): Unit = put(key, value, true)

    def update(f: (VAL) => VAL): Unit = {
      var hashPos = 0
      while (hashPos + rowLen <= data.capacity) {
        val hash = data.getInt(hashPos)
        if (hash != 0 && hash != Int.MinValue) {
          f(getValue(hashPos)) match {
            case null => data.putInt(hashPos, Int.MinValue)
            case newValue => putValue(hashPos, newValue)
          }
        }
        hashPos += rowLen
      }
    }

    def iterator = new Iterator[(ByteBuffer, VAL)] {
      private var hashPos = 0
      private val keyBuffer = data.duplicate

      override def hasNext: Boolean = {
        while (hashPos + rowLen <= data.capacity) {
          val hash = data.getInt(hashPos)
          if (hash != 0 && hash != Int.MinValue) {
            return true
          } else {
            hashPos += rowLen
          }
        }
        false
      }

      override def next(): (ByteBuffer, (Boolean, Short, Int)) = {
        if (hashPos + rowLen > data.capacity) throw new NoSuchElementException
        keyBuffer.limit(hashPos + keyLen)
        keyBuffer.position(hashPos)
        val result = (keyBuffer, getValue(hashPos))
        hashPos += rowLen
        result
      }
    }

    private[logmap] def find(key: ByteBuffer): Int = {
      val hashCode = key.getInt(key.position)
      if (hashCode == 0 || hashCode == Int.MinValue) {
        throw new IllegalArgumentException(s"Invalid hashCode `${hashCode}`. hashCode cannot be 0 or ${Int.MinValue}")
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

    private def getValue(hashPos: Int, fromData: ByteBuffer = data): VAL = {
      (fromData.get(hashPos + keyLen) != 0, fromData.getShort(hashPos + keyLen + 1), fromData.getInt(hashPos + keyLen + 3))
    }

    private def putValue(hashPos: Int, value: VAL): Unit = {
      data.put(hashPos + keyLen, if (value._1) 1 else 0)
      data.putShort(hashPos + keyLen + 1, value._2)
      data.putInt(hashPos + keyLen + 3, value._3)
    }

    private def put(key: ByteBuffer, value: VAL, growable: Boolean): Boolean = {
      val hashCode = key.getInt(key.position)
      if (hashCode == 0 || hashCode == Int.MinValue) {
        throw new IllegalArgumentException(s"Invalid hashCode `${hashCode}`. hashCode cannot be 0 or ${Int.MinValue}")
      }
      var hash = getHash(hashCode)
      var numCollisions = 0
      while (true) {
        val hashPos = hash * rowLen
        val inspectHashCode = data.getInt(hashPos)
        if (inspectHashCode == 0 || inspectHashCode == Int.MinValue || (inspectHashCode == hashCode && keyEquals(hashPos, key))) {
          if (inspectHashCode == 0 || inspectHashCode == Int.MinValue) loaded += 1
          var i = 0
          while (i < keyLen) {
            data.put(hashPos + i, key.get(key.position + i))
            i += 1
          }
          putValue(hashPos, value)
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
      return false
    }

    private def getHash(hashCode: Int): Int = {
      if (hashCode == Integer.MIN_VALUE || hashCode == 0) {
        Integer.MAX_VALUE % numPositions
      } else {
        ((hashCode.toLong + Integer.MAX_VALUE) % numPositions) match {
          case h if (h >= numPositions) => throw new IllegalArgumentException
          case h => h.toInt
        }
      }
    }

    private def resolveCollision(hash: Int): Int = {
      //open addressing:
      (hash + 1) % numPositions
      //not used for now:
      // - Coalesced hashing
      // - Cuckoo hashing
      // - 2-choice hashing
      // - Hopscotch hashing
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
        capacity = (capacity * 2.0).toInt / rowLen * rowLen
        data = ByteBuffer.allocateDirect(capacity)
        numPositions = capacity / rowLen
        loaded = 0
        maxCollisions = 0
        if (oldData != null && oldData.capacity > 0) {
          var c = 0
          while (c < oldData.capacity) {
            val hashCode = oldData.getInt(c)
            if (hashCode != 0 && hashCode != Int.MinValue) {
              oldData.position(c)
              success = success && put(oldData, getValue(c, oldData), false)
            }
            c += rowLen
          }
        }
      } while (!success)
      true
    }

  }

}
