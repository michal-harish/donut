package org.apache.donut.memstore.lz4

import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
 * Created by mharis on 01/10/15.
 */
class IntIndexTest extends FlatSpec with Matchers {

  val index = new IntIndex(65)

  index.capacityInBytes should be (0)

  (0 to 15).foreach(i => {
    index.put(1) should be (i)
    index.capacityInBytes should be(65)
    index.get(i) should be(1)
  })

  (16 to 31).foreach(i => {
    index.put(2) should be (i)
    index.capacityInBytes should be(130)
    index.get(i) should be(2)
  })

  (0 to 31).foreach(i => {
    index.get(i) should be (i / 16 + 1)
  })
}
