package net.imagini.dxp.donut

import org.apache.donut.LocalStorage
import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
 * Created by mharis on 14/09/15.
 */
class LocalStorageTest extends FlatSpec with Matchers {

  val storage = new LocalStorage[String](3)
  storage.put("9", "A")
  storage.put("8", "B")
  storage.put("7", "C") // after 3 puts the order of eviction is 9,8,7
  storage.get("9") // refreshes 9 so the order of eviction is 8,7,9
  storage.put("6", "D") // pushes out 8
  storage.put("5", "E") // pushes out 7

  storage.size should be (3)
  storage.contains("8") should be (false)
  storage.contains("7") should be (false)
  storage.contains("9") should be (true)
  storage.contains("6") should be (true)
  storage.contains("5") should be (true)

}
