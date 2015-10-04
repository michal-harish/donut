package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._


/**
 * Created by mharis on 01/10/15.
 *
 * This is a specialised Thread-Safe HashMap based which may be partially compressed.
 */
class ConcurrentLogHashMap(val maxSegmentSizeMb: Int, val compressMinBlockSize: Int) {

  private[lz4] val catalog = new Catalog(maxSegmentSizeMb, compressMinBlockSize)

  //  private val index = new util.HashMap[ByteBuffer, (Boolean, Short, Int)] //heap-based index for testing

  private val index = new VarHashTable(initialCapacityKb = 64)
  private val indexLock = new ReentrantReadWriteLock
  private val indexReader = indexLock.readLock
  private val indexWriter = indexLock.writeLock

  def put(key: ByteBuffer, value: ByteBuffer) = {
    val newIndexValue = catalog.append(key, value)
    indexWriter.lock
    try {
      val previousIndexValue = index.get(key)
      index.put(key, newIndexValue)
      if (previousIndexValue != null) {
        catalog.removeBlock(previousIndexValue)
      }
    } finally {
      indexWriter.unlock
    }
  }

  def get(key: ByteBuffer): ByteBuffer = get(key, (b: ByteBuffer) => b)

  def get[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    indexReader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case i: (Boolean, Short, Int) => {
          val indexValue = new IndexValue(i)
          if (catalog.isInLastSegment(indexValue)) {
            return catalog.getBlock(indexValue, mapper, null)
          } else {
            var oldIndexValue: IndexValue = null
            var newIndexValue: IndexValue = null
              indexReader.unlock
              indexWriter.lock
              try {
                oldIndexValue = new IndexValue(index.get(key))
                if (oldIndexValue.inTransit) {
                  return catalog.getBlock(oldIndexValue, mapper, null)
                }
                index.put(key, oldIndexValue.setTransit)
                newIndexValue = catalog.alloc(catalog.sizeOf(oldIndexValue))
              } catch {
                case e: Throwable => {
                  if (oldIndexValue != null) index.put(key, oldIndexValue.unsetTransit)
                  if (newIndexValue != null) catalog.dealloc(newIndexValue)
                  throw e
                }
              } finally {
                indexReader.lock
                indexWriter.unlock
              }

            catalog.move(oldIndexValue, newIndexValue)

            indexReader.unlock
            indexWriter.lock
            try {
              index.put(key, newIndexValue)
            } finally {
              indexReader.lock
              indexWriter.unlock
            }
            catalog.getBlock(newIndexValue, mapper, null)
          }
        }
      }
    } finally {
      indexReader.unlock
    }
  }

  def numSegments = catalog.safeIterator.size

  def count: Int = catalog.safeIterator.map(_.count).sum

  def compressRatio: Double = catalog.safeIterator.map(_.compressRatio).toSeq match {
    case rates => if (rates.size == 0) 0 else (rates.sum / rates.size)
  }

  def sizeInBytes: Long = catalog.safeIterator.map(_.sizeInBytes.toLong).sum + index.sizeInBytes

  def compact = catalog.compact

}

/**
 * @param segment
 * @param block
 * @param inTransit this is an internal used to improve concurrency of get-touch
 */
class IndexValue(val segment: Short, val block: Int, val inTransit: Boolean = false)
  extends Tuple3[Boolean, Short, Int](inTransit, segment, block) {
  def this(t3: (Boolean, Short, Int)) = this(t3._2, t3._3, t3._1)
  def setTransit = (true, segment, block)
  def unsetTransit = (false, segment, block)
}

class Catalog(val maxSegmentSizeMb: Int, val compressMinBlockSize: Int) extends AnyRef {

  //TODO run compaction in the background : 1) closing full segments 2) remove empty segments

  private val segments = new java.util.LinkedList[Segment]()

  forceNewSegment

  /**
   * @param iv
   * @return true if the index value is in transit to the latest semgnet or it already is int that segment
   */
  def isInLastSegment(iv: (Boolean, Short, Int)): Boolean = iv._1 || (segments.size - 1).toShort == iv._2

  //TODO this iterator is not really safe yet - just a placeholder method
  def safeIterator = segments.iterator.asScala

  //this is for testing only
  def forceNewSegment = segments.synchronized(segments.add(createNewSegment))


  def sizeOf(i: IndexValue): Int = segments.get(i.segment).sizeOf(i.block)

  def getBlock[X](i: IndexValue, mapper: ByteBuffer => X, buffer: ByteBuffer): X = {
    segments.get(i.segment).get(i.block, mapper, buffer)
  }
  
  def removeBlock(coord: (Boolean, Short, Int)) = segments.get(coord._2).remove(coord._3)

  private def createNewSegment = new SegmentDirectMemoryLZ4(maxSegmentSizeMb, compressMinBlockSize)

  /**
   * Warning: This is a dangerous method that can lead to memory leak if not used properly.
   * @param wrappedLength
   * @return
   */
  def alloc(wrappedLength: Int): IndexValue = {
    (segments.size - 1).toShort match {
      case current: Short => segments.get(current).alloc(wrappedLength) match {
        case newBlock if (newBlock >= 0) => new IndexValue(current, newBlock)
        case _ => segments.synchronized {
          (segments.size - 1).toShort match {
            case currentChecked => segments.get(currentChecked).alloc(wrappedLength) match {
              case newBlock if (newBlock >= 0) => new IndexValue(currentChecked, newBlock)
              case _ => {
                val newSegment = createNewSegment
                newSegment.alloc(wrappedLength) match {
                  case -1 => throw new IllegalArgumentException("Value is bigger than segment size " + newSegment.capacityMb + " Mb")
                  case newBlock => {
                    segments.add(newSegment)
                    new IndexValue((segments.size - 1).toShort, newBlock)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def move(iSrc: IndexValue, iDst: IndexValue): Unit = {
    val srcSegment = segments.get(iSrc.segment)
    val dstSegment = segments.get(iDst.segment)
    dstSegment.copy(srcSegment, iSrc.block, iDst.block)
    srcSegment.remove(iSrc.block)
  }

  def dealloc(i: IndexValue): Unit = {
    segments.get(i.segment).remove(i.block)
  }

  def append(key: ByteBuffer, value: ByteBuffer): IndexValue = {
    if (value.remaining < compressMinBlockSize) {
      //if we know that the value is not going to be compressed, using alloc will lock significantly less
      val newIndexValue = alloc(value.remaining)
      try {
        segments.get(newIndexValue.segment).set(newIndexValue.block, value)
      } catch {
        case e: Throwable => {
          dealloc(newIndexValue)
          throw e
        }
      }
      newIndexValue
    } else {
      (segments.size - 1).toShort match {
        case current: Short => segments.get(current).put(value) match {
          case newBlock if (newBlock >= 0) => new IndexValue(current, newBlock)
          case _ => segments.synchronized {
            (segments.size - 1).toShort match {
              case currentChecked => segments.get(currentChecked).put(value) match {
                case newBlock if (newBlock >= 0) => new IndexValue(currentChecked, newBlock)
                case _ => {
                  val newSegment = createNewSegment
                  newSegment.put(value) match {
                    case -1 => throw new IllegalArgumentException("Value is bigger than segment size " + newSegment.capacityMb + " Mb")
                    case newBlock => {
                      segments.add(newSegment)
                      new IndexValue((segments.size -1).toShort, newBlock)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def compact = {
    var i = segments.size - 1
    while (i >= 0) {
      val segment = segments.get(i)
      segment.compact
      if (segment.count == 0) {
        //TODO reuse segment
      }
      i -= 1
    }
  }

}


