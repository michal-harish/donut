package net.imagini.graphstream.common

/**
 * Created by mharis on 10/09/15.
 *
 * HE - HGraphEdge - is a lightweight object that holds the properties of each edge in the HGraph
 */

class Edge(val bytes: Array[Byte], val vendorCode: Short, val ts: Long) extends java.io.Serializable with Combinable[Edge] {

  override def combine(other: Edge): Edge = Edge.applyVersion(ByteUtils.max(bytes, other.bytes), Math.max(other.ts, ts))

  def hbaseValue: (Array[Byte], Long) = (bytes, ts)

  def vendorUnknown: Boolean = vendorCode == Edge.VENDOR_CODE_UNKNOWN

  def probability: Double = (bytes(1) & 0xFF).toDouble / 255.0

  override def toString: String = s"P=${probability}@${Edge.vendors(vendorCode)}/${ts}"

  override def hashCode = ByteUtils.asIntValue(bytes)

  override def equals(other: Any) = other != null && other.isInstanceOf[Edge] && ByteUtils.equals(bytes, other.asInstanceOf[Edge].bytes)
}


object Edge extends java.io.Serializable {
  val CURRENT_VERSION = 1.toByte
  val VENDOR_CODE_UNKNOWN = 0.toShort
  val vendors = Map[Short, String](
    VENDOR_CODE_UNKNOWN -> "UNKNOWN",
    //PROBABILISTIC
    128.toShort -> "S6",
    129.toShort -> "CROSSWISE",
    //DETERMINISTIC
    250.toShort -> "AAT",
    //RESERVED
    251.toShort -> "test1",
    252.toShort -> "test2",
    253.toShort -> "test3",
    254.toShort -> "test4",
    Short.MaxValue -> "RESERVED")
  val vendors2: Map[String, Short] = vendors.map(x => (x._2 -> x._1))

  implicit def ordering[A <: (Vid, _)]: Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = x._1.compareTo(y._1)
  }

  def apply(vendorCode: Short, probability: Double, ts: Long): Edge = {
    val b: Array[Byte] = new Array[Byte](4)
    b(0) = CURRENT_VERSION
    b(1) = (probability * 255.0).toByte
    b(2) = ((vendorCode >>> 8) & 0xFF).toByte
    b(3) = ((vendorCode >>> 0) & 0xFF).toByte
    new Edge(b, vendorCode, ts)
  }

  def apply(vendor: String, probability: Double, ts: Long): Edge = {
    if (probability < 0.0 || probability > 1.0) throw new IllegalArgumentException(s"Probability must be 0.0 >= p >= 1.0")
    if (!vendors2.contains(vendor)) throw new IllegalArgumentException(s"Unknown graph connection vendor `${vendor}`")
    apply(vendors2(vendor), probability, ts)
  }

  def apply(vendor: String, probability: Double): Edge = apply(vendor, probability, Long.MaxValue)

  def applyVersion(bytes: Array[Byte], ts: Long): Edge = {
    if (bytes.length != 4 || bytes(0) != CURRENT_VERSION) {
      apply(Edge.VENDOR_CODE_UNKNOWN, 255.toByte, ts)
    } else {
      val vendorCodeParsed: Short = (((bytes(2).toShort & 0xff) << 8) + ((bytes(3).toShort & 0xff) << 0)).toShort
      if (vendors.contains(vendorCodeParsed)) {
        new Edge(bytes, vendorCodeParsed, ts)
      } else {
        apply(Edge.VENDOR_CODE_UNKNOWN, bytes(1), ts)
      }
    }
  }

}


