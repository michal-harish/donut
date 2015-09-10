package net.imagini.dxp.common

trait Cookie {}

trait Mobile {}

trait Internal {}

object IdSpace {
  val vdna = "vdna".hashCode.toShort

  def apply(id: Array[Byte], offset: Int, length: Int): Short = {
    (((id(offset + 4) & 0xff) << 8) + (id(offset + 5) & 0xff)).toShort
  }

  def apply(idSpace: String): Short = idSpace.hashCode.toShort

  def apply(idSpace: Short): IdSpace = mapping(idSpace)

  def exists(idSpace: Short): Boolean = mapping.contains(idSpace)

  val mapping = Map[Short, IdSpace](
    //test identifiers
    new IdSpaceLong("test").keyValue //for unit tests and system tests
    ,new IdSpaceString("test2").keyValue //for unit tests and system tests
    ,new IdSpaceLongHash("test3").keyValue //for unit tests and system tests

    //universal identifiers
    , (new IdSpaceUUID("idfa") with Mobile).keyValue // Apple IDFA
    , (new IdSpaceUUID("aaid") with Mobile).keyValue // Android Advertising ID
    , (new IdSpaceHEX("aidsha1") with Mobile).keyValue //Android SHA1 hexadecimal of the legacy AID
    , (new IdSpaceHEX("aidmd5") with Mobile).keyValue //Android MD5 hexadecimal of the legacy AID
    , (new IdSpaceUUIDNumeric("waid") with Mobile).keyValue // Windows Adevertising ID, e.g. `2E5BA36FE8E24862BC89CD394440339C`

    //email-derived identifiers
//    , new IdSpaceEmailHash("mp2eh", "4eXa!Re28Me_E?U+EMefrupu2ucrA5e4E=@y6C7em9BU?@hAN5MeBuQadr=WuSTu").keyValue
//    , new IdSpaceEmailHash("cu2eh", "CDA1A024B3F7255F1C2DBE1733D43DF02D558DD496D2EE9773FD27F96E168C30").keyValue
//    , new IdSpaceEmailHash("lso2eh", "sAj4bRaFraquz2R@wr6MEchAthu$eFUS7DuCrecAjustuK$X-6a_A_RUprucrakA").keyValue
//    , new IdSpaceEmailHash("dv2eh", "puxAdrAcu6=KeFUs$t8232NEs5DRAp3EQUDRaVaxa9Racra8a5ubefaswudre9aX").keyValue

    //cookie identifiers
    , (new IdSpaceUUID("vdna") with Cookie).keyValue
    , (new IdSpaceLongPositive("a") with Cookie).keyValue
    , (new IdSpaceString("d") with Cookie).keyValue
    , (new IdSpaceString("r") with Cookie).keyValue
//    , (new IdSpaceUUID("v") with Cookie).keyValue
//    , (new IdSpaceUUID("x") with Cookie).keyValue
//    , (new IdSpaceLongHash("rf") with Cookie).keyValue
//    , (new IdSpaceUUID("adz_id") with Cookie).keyValue
//    , (new IdSpaceString("p") with Cookie).keyValue
//    , (new IdSpaceString("qbit_id") with Cookie).keyValue
    //, (new IdSpaceString("l") with Cookie).keyValue //TODO //"TA"UUID or 19rhld70kucbsa, e.g. both serdes
    //, (new IdSpace...("m") with Cookie).keyValue,   //TODO 2 hotspots, otherwise distributed evenly
    //, (new IdSpaceLong("tu_id") with ???),          //TODO very bad distribution
    //, (new IdSpaceLong("ne_id") with ???),          //TODO very bad distribution
    //, (new IdSpaceLong("tm_id") with ???),          //TODO looks ok but check why it works and other longs don't
    //removed - not useful hash of ipv4 (new IdSpaceString("k") with Cookie).keyValue

    //internal vendor identifiers
//    , (new IdSpaceUUIDNumericNoLeadZeros("ltm_id") with Cookie).keyValue
//    , (new IdSpaceUUIDNumeric("i") with Cookie).keyValue
    , (new IdSpaceUUIDNumeric("fu_id2") with Internal).keyValue //not found in syncs, crm validation set
    , (new IdSpaceUUIDNumeric("tman_id2") with Internal).keyValue //not found in syncs, crm validation set
    , (new IdSpaceUUIDNumeric("tshop_id2") with Internal).keyValue //not found in syncs, crm validation set
    , (new IdSpaceLong("w_id") with Internal).keyValue //not found in syncs, crm validation set
    , (new IdSpaceLong("fu_id") with Internal).keyValue //not found in syncs, crm validation set
    , (new IdSpaceLong("tman_id") with Internal).keyValue //not found in syncs, crm validation set
    , (new IdSpaceLong("tshop_id") with Internal).keyValue //not found in syncs, crm validation set
  )
  val symbols: Iterable[String] = mapping.values.map(_.symbol)
}

abstract class IdSpace(val symbol: String) {
  val i = symbol.hashCode.toShort

  def asBytes(id: String): Array[Byte]

  def asString(bytes: Array[Byte]): String

  final def toString(bytes: Array[Byte]): String = asString(bytes) + ":" + symbol

  def allocate(length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length + 6)
    bytes(4) = (i >>> 8).toByte
    bytes(5) = (i).toByte
    bytes
  }

  def keyValue: (Short, IdSpace) = (i -> this)
}

class IdSpaceUUID(symbol: String) extends IdSpace(symbol) with VidSerdeUUID {
  override def asString(bytes: Array[Byte]): String = uuidToString(bytes, 6)

  override def asBytes(id: String): Array[Byte] = {
    try {
      val bytes = allocate(16)
      stringToUUID(id, 0, bytes, 6)
      ByteUtils.copy(bytes, 6, bytes, 0, 4)
      bytes
    } catch {
      case e: IllegalArgumentException => {
        throw new IllegalArgumentException(e.getMessage + s" for idSpace `${symbol}`")
      }
    }
  }
}

class IdSpaceUUIDNumeric(symbol: String) extends IdSpace(symbol) with VidSerdeUUIDNumeric {
  override def asString(bytes: Array[Byte]): String = uuidToNumericString(bytes, 6)

  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(16)
    stringToUUIDNumeric(id, 0, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}

class IdSpaceUUIDNumericNoLeadZeros(symbol: String) extends IdSpace(symbol) with VidSerdeUUIDNumeric {
  override def asString(bytes: Array[Byte]): String = uuidToNumericString(bytes, 6).dropWhile(_ == '0')

  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(16)
    stringToUUIDNumeric("00000000000000000000000000000000" + id takeRight 32, 0, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}


class IdSpaceString(symbol: String) extends IdSpace(symbol) with VidSerdeString {
  override def asString(bytes: Array[Byte]): String = bytesToString(bytes, 6, bytes.length)

  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(id.length)
    stringToBytes(id, 0, id.length, bytes, 6)
    ByteUtils.putIntValue(id.hashCode, bytes, 0)
    bytes
  }

}

class IdSpaceLong(symbol: String) extends IdSpace(symbol) with VidSerdeLong {
  override def asString(bytes: Array[Byte]): String = longBytesToString(bytes, 6)

  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(8)
    longStringToBytes(id, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}

class IdSpaceLongHash(symbol: String) extends IdSpace(symbol) with VidSerdeLong {
  override def asString(bytes: Array[Byte]): String = longBytesToString(bytes, 6)

  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(8)
    longStringToBytes(id, bytes, 6)
    ByteUtils.putIntValue(id.hashCode, bytes, 0)
    bytes
  }
}

class IdSpaceLongPositive(symbol: String) extends IdSpace(symbol) with VidSerdeLongPositive {
  override def asString(bytes: Array[Byte]): String = longPositiveBytesToString(bytes, 6)

  override def asBytes(id: String): Array[Byte] = {
    val bytes = allocate(8)
    longPositiveStringToBytes(id, bytes, 6)
    ByteUtils.copy(bytes, 6, bytes, 0, 4)
    bytes
  }
}

class IdSpaceEmailHash(symbol: String, val salt: String) extends IdSpaceUUIDNumeric(symbol) {

}


class IdSpaceHEX(symbol: String) extends IdSpace(symbol) with VidSerdeHEX {
  override def asBytes(id: String): Array[Byte] = {
    if (id.length % 2 != 0) throw new IllegalArgumentException
    val bytes = allocate(id.length / 2)
    hexadecimalToBytes(id, 0, id.length, bytes, 6)
    val crc = ByteUtils.crc32(bytes, 6, bytes.length - 6)
    ByteUtils.putIntValue(crc, bytes, 0)
    bytes
  }

  override def asString(bytes: Array[Byte]): String = {
    bytesToHexadecimal(bytes, 6, bytes.length - 6)
  }
}

trait VidSerdeUUID {
  val uuidPattern = "^(?i)[a-f0-9]{8}\\-[a-f0-9]{4}\\-[a-f0-9]{4}\\-[a-f0-9]{4}\\-[a-f0-9]{12}$".r.pattern

  def stringToUUID(id: String, srcOffset: Int, dest: Array[Byte], destOffset: Int): Array[Byte] = {
    if (uuidPattern.matcher(id).matches) ByteUtils.parseUUID(id.getBytes(), 1, srcOffset, dest, destOffset)
    else throw new IllegalArgumentException(s"UUID string format found: ${id}")
    dest
  }

  def uuidToString(src: Array[Byte], srcOffset: Int): String = {
    ByteUtils.UUIDToString(src, srcOffset)
  }
}

trait VidSerdeUUIDNumeric {
  val uuidPatternNumeric = "^(?i)[a-f0-9]{32}$".r.pattern

  def stringToUUIDNumeric(id: String, srcOffset: Int, dest: Array[Byte], destOffset: Int) {
    if (uuidPatternNumeric.matcher(id).matches) ByteUtils.parseUUID(id.getBytes(), 0, srcOffset, dest, destOffset)
    else throw new IllegalArgumentException(s"Numeric UUID string format found: ${id}")
  }

  def uuidToNumericString(src: Array[Byte], srcOffset: Int): String = {
    ByteUtils.UUIDToNumericString(src, srcOffset)
  }
}

trait VidSerdeString {
  def stringToBytes(id: String, srcStart: Int, srcEnd: Int, dest: Array[Byte], destOffset: Int) {
    id.getBytes(srcStart, srcEnd, dest, destOffset)
  }

  def bytesToString(src: Array[Byte], srcStart: Int, srcEnd: Int): String = {
    new String(src.slice(srcStart, srcEnd))
  }
}

trait VidSerdeLong {
  def longStringToBytes(id: String, dest: Array[Byte], destOffset: Int) {
    ByteUtils.putLongValue(ByteUtils.parseLongRadix10(id.getBytes, 0, id.length - 1), dest, destOffset)
  }

  def longBytesToString(bytes: Array[Byte], offset: Int): String = {
    ByteUtils.asLongValue(bytes, offset).toString
  }
}

trait VidSerdeLongPositive {
  def longPositiveStringToBytes(id: String, dest: Array[Byte], destOffset: Int) = {
    ByteUtils.putLongValue(ByteUtils.parseLongRadix10(id.getBytes, 0, id.length - 1) << 1, dest, destOffset)
  }

  def longPositiveBytesToString(bytes: Array[Byte], offset: Int): String = {
    (ByteUtils.asLongValue(bytes, offset) >>> 1).toString
  }
}

trait VidSerdeHEX {
  def hexadecimalToBytes(id: String, srcOffset: Int, srcLen: Int, dest: Array[Byte], destOffset: Int) = {
    ByteUtils.parseRadix16(id.getBytes, srcOffset, srcLen, dest, destOffset)
  }

  def bytesToHexadecimal(bytes: Array[Byte], offset: Int, len: Int) = {
    ByteUtils.toRadix16(bytes, offset, len)
  }
}


//TODO trait VidSerdeInt
//TODO trait VidSerdeDouble
//TODO trait VidSerdeIPV4

