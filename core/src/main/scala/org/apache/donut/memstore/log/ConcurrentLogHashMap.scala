package org.apache.donut.memstore.log

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._


/**
 * Created by mharis on 01/10/15.
 *
 * This is a specialised Thread-Safe HashMap based which may be partially compressed.
 */
class ConcurrentLogHashMap(val segmentSizeMb: Int, val compressMinBlockSize: Int) {

  private[log] val catalog = new Catalog(segmentSizeMb, compressMinBlockSize)

  def numSegments = catalog.iterator.size

  def count: Int = catalog.iterator.map(_.count).sum

  def compressRatio: Double = catalog.iterator.map(_.compressRatio).toSeq match {case r => if (r.size == 0) 0 else (r.sum / r.size)}

  def capacity: Long =  catalog.iterator.map(_.capacityInBytes.toLong).sum + index.sizeInBytes

  def sizeInBytes: Long = catalog.iterator.map(_.sizeInBytes.toLong).sum + index.sizeInBytes

  def load: Double = sizeInBytes.toDouble / capacity

  def compact = catalog.compact

  private val index = new VarHashTable(initialCapacityKb = 64, loadFactor = 0.5)
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
          if (catalog.isInLastSegment(indexValue) || indexValue.inTransit) {
            return catalog.getBlock(indexValue, mapper)
          } else {
            var oldIndexValue: IndexValue = null
            var newIndexValue: IndexValue = null
              indexReader.unlock
              indexWriter.lock
              try {
                oldIndexValue = new IndexValue(index.get(key))
                if (oldIndexValue.inTransit) {
                  return catalog.getBlock(oldIndexValue, mapper)
                }
                index.flag(key, true)
                newIndexValue = catalog.alloc(catalog.sizeOf(oldIndexValue))
              } catch {
                case e: Throwable => {
                  if (oldIndexValue != null) index.flag(key, false)
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
            catalog.getBlock(newIndexValue, mapper)
          }
        }
      }
    } finally {
      indexReader.unlock
    }
  }

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

class Catalog(val segmentSizeMb: Int, val compressMinBlockSize: Int) extends AnyRef {

  //TODO configuration for VarHashTable index so that we do not grow it pointlessly if we know how big it's going to get

  //TODO run compaction in the background

  private val segments = new java.util.LinkedList[Segment]()

  @volatile private var currentSegment: Short = - 1

  createNewSegment

  /**
   * @param i
   * @return true if the index value belongs to the latest segment
   */
  def isInLastSegment(i: (Boolean, Short, Int)): Boolean = i._2 == currentSegment

  def iterator: Iterator[Segment] = segments.synchronized(segments.iterator.asScala.toList).iterator

  def sizeOf(i: (Boolean, Short, Int)): Int = segments.get(i._2).sizeOf(i._3)

  def getBlock[X](i: (Boolean, Short, Int), mapper: ByteBuffer => X): X = {
    segments.get(i._2).get(i._3, mapper)
  }

  def removeBlock(coord: (Boolean, Short, Int)) = segments.get(coord._2).remove(coord._3)

  private[log] def createNewSegment: Short = segments.synchronized {
    for(s <- 0 to segments.size-1) {
      if (segments.get(s).count == 0) { //reuse segment that was compacted-out
        currentSegment = s.toShort
        return currentSegment
      }
    }
    segments.add(new SegmentDirectMemoryLZ4(segmentSizeMb, compressMinBlockSize))
    currentSegment = (segments.size - 1).toShort
    return currentSegment
  }

  def compact = {
    var i = segments.size - 1
    while (i >= 0) {
      val segment = segments.get(i)
      segment.compact
      i -= 1
    }
  }

  /**
   * Warning: This is a dangerous method that can lead to memory leak if not used properly.
   * @param wrappedLength
   * @return
   */
  def alloc(wrappedLength: Int): IndexValue = {
    currentSegment match {
      case current: Short => segments.get(current).alloc(wrappedLength) match {
        case newBlock if (newBlock >= 0) => new IndexValue(current, newBlock)
        case _ => segments.synchronized {
          currentSegment match {
            case currentChecked => segments.get(currentChecked).alloc(wrappedLength) match {
              case newBlock if (newBlock >= 0) => new IndexValue(currentChecked, newBlock)
              case _ => {
                val newSegment = createNewSegment
                segments.get(newSegment).alloc(wrappedLength) match {
                  case -1 => throw new IllegalArgumentException(s"Value of length `${wrappedLength}` is bigger than segment size " + segmentSizeMb + " Mb")
                  case newBlock => new IndexValue(newSegment, newBlock)
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
      currentSegment match {
        case current: Short => segments.get(current).put(value) match {
          case newBlock if (newBlock >= 0) => new IndexValue(current, newBlock)
          case _ => segments.synchronized {
            currentSegment match {
              case currentChecked => segments.get(currentChecked).put(value) match {
                case newBlock if (newBlock >= 0) => new IndexValue(currentChecked, newBlock)
                case _ => {
                  val newSegment = createNewSegment
                  segments.get(newSegment).put(value) match {
                    case -1 => throw new IllegalArgumentException("Value is bigger than segment size " + segmentSizeMb + " Mb")
                    case newBlock => new IndexValue(newSegment, newBlock)
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


