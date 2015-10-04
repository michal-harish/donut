package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConverters._


/**
 * Created by mharis on 01/10/15.
 *
 * This is a specialised Thread-Safe HashMap based which may be partially compressed.
 */
class ConcurrentLogHashMap(val maxSegmentSizeMb: Int, val compressMinBlockSize: Int) {

  private[lz4] val catalog = new Catalog(maxSegmentSizeMb, compressMinBlockSize)

  // TODO index should be off-heap private
  //val index2 = new VarHashTable(initialCapacityKb = 64)

  private val index = new util.HashMap[ByteBuffer, IndexValue]
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
        previousIndexValue.segment.remove(previousIndexValue.block)
      }
    } finally {
      indexWriter.unlock
    }
  }

  def get(key: ByteBuffer): ByteBuffer = get(key, (b: ByteBuffer) => b)

  val SIMPLE_GET = false

  def get[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    indexReader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case indexValue: IndexValue => {
          if (SIMPLE_GET || catalog.isInLastSegment(indexValue)) {
            return indexValue.segment.get(indexValue.block, mapper, null)
          } else {
            var oldIndexValue: IndexValue = null
            var newIndexValue: IndexValue = null
            try {
              indexReader.unlock
              indexWriter.lock
              try {
                oldIndexValue = index.get(key)
                if (oldIndexValue.inTransit) {
                  return oldIndexValue.segment.get(oldIndexValue.block, mapper, null)
                }
                oldIndexValue.inTransit = true
                newIndexValue = catalog.alloc(oldIndexValue.segment.sizeOf(oldIndexValue.block))
              } finally {
                indexReader.lock
                indexWriter.unlock
              }
            } catch {
              case e: Throwable => {
                if (oldIndexValue != null) oldIndexValue.inTransit = true
                if (newIndexValue != null) catalog.dealloc(newIndexValue)
                throw e
              }
            }

            catalog.move(oldIndexValue, newIndexValue)

            indexReader.unlock
            indexWriter.lock
            try {
              index.put(key, newIndexValue)
              oldIndexValue.inTransit = false
            } finally {
              indexReader.lock
              indexWriter.unlock
            }

            newIndexValue.segment.get(newIndexValue.block, mapper, null)
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

  def sizeInBytes: Long = catalog.safeIterator.map(_.sizeInBytes.toLong).sum

  def compact = catalog.compact

}

/**
 * @param segment
 * @param block
 * @param inTransit this is an internal used to improve concurrency of get-touch
 */
case class IndexValue(val segment: Segment, val block: Int, var inTransit: Boolean = false)

class Catalog(val maxSegmentSizeMb: Int, val compressMinBlockSize: Int) extends AnyRef {

  //TODO run compaction in the background : 1) closing full segments 2) remove empty segments

  private val segments = new java.util.LinkedList[Segment]()

  forceNewSegment

  //TODO is this safe ?
  def isInLastSegment(iv: IndexValue): Boolean = iv.inTransit || segments.get(segments.size - 1) == iv.segment

  //TODO this iterator is not really safe yet - just a placeholder method
  def safeIterator = segments.iterator.asScala

  //this is for testing only
  def forceNewSegment = segments.synchronized(segments.add(createNewSegment))

  private def createNewSegment = new SegmentDirectMemoryLZ4(maxSegmentSizeMb, compressMinBlockSize)

  /**
   * Warning: This is a dangerous method that can lead to memory leak if not used properly.
   * @param wrappedLength
   * @return
   */
  def alloc(wrappedLength: Int): IndexValue = {
    segments.get(segments.size - 1) match {
      case current: Segment => current.alloc(wrappedLength) match {
        case newBlock if (newBlock >= 0) => new IndexValue(current, newBlock)
        case _ => segments.synchronized {
          segments.get(segments.size - 1) match {
            case currentChecked => current.alloc(wrappedLength) match {
              case newBlock if (newBlock >= 0) => new IndexValue(currentChecked, newBlock)
              case _ => {
                val newSegment = createNewSegment
                newSegment.alloc(wrappedLength) match {
                  case -1 => throw new IllegalArgumentException("Value is bigger than segment size " + newSegment.capacityMb + " Mb")
                  case newBlock => {
                    segments.add(newSegment)
                    new IndexValue(newSegment, newBlock)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def move(srcIndexValue: IndexValue, dstIndexValue: IndexValue): Unit = {
    dstIndexValue.segment.copy(srcIndexValue.segment, srcIndexValue.block, dstIndexValue.block)
    srcIndexValue.segment.remove(srcIndexValue.block)
  }

  def dealloc(indexValue: IndexValue): Unit = indexValue.segment.remove(indexValue.block)

  def append(key: ByteBuffer, value: ByteBuffer): IndexValue = {
    if (value.remaining < compressMinBlockSize) {
      //if we know that the value is not going to be compressed, using alloc will lock significantly less
      val newIndexValue = alloc(value.remaining)
      try {
        newIndexValue.segment.set(newIndexValue.block, value)
      } catch {
        case e: Throwable => {
          dealloc(newIndexValue)
          throw e
        }
      }
      newIndexValue
    } else {
      segments.get(segments.size - 1) match {
        case current: Segment => current.put(value) match {
          case newBlock if (newBlock >= 0) => new IndexValue(current, newBlock)
          case _ => segments.synchronized {
            segments.get(segments.size - 1) match {
              case currentChecked => currentChecked.put(value) match {
                case newBlock if (newBlock >= 0) => new IndexValue(currentChecked, newBlock)
                case _ => {
                  val newSegment = createNewSegment
                  newSegment.put(value) match {
                    case -1 => throw new IllegalArgumentException("Value is bigger than segment size " + newSegment.capacityMb + " Mb")
                    case newBlock => {
                      segments.add(newSegment)
                      new IndexValue(newSegment, newBlock)
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
        segments.remove(i) //FIXME this is not concurrent
      }
      i -= 1
    }
  }
}


