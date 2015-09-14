package net.imagini.dxp.donut

import org.apache.donut.LocalStorage
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.collection.JavaConverters._

/**
 * Created by mharis on 14/09/15.
 */
class LocalStorageTest extends FlatSpec with Matchers {

  val storage = new LocalStorage[String](3)
  storage.put("9", "A")
  storage.put("8", "B")
  storage.put("7", "C")

  storage.size should be (3)

  storage.underlying.entrySet.asScala.foreach(println)
  //storage.head should be ()
}
