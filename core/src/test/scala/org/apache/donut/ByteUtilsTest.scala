package org.apache.donut

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by mharis on 31/07/15.
  */
class ByteUtilsTest extends FlatSpec with Matchers {

   val input = "----020ac416f90d91cffc09b56a9e7aea0420e0cf59----"

   val b = ByteUtils.parseRadix16(input.getBytes, 4, 40)

   ByteUtils.toRadix16(b, 0, 20) should be ("020ac416f90d91cffc09b56a9e7aea0420e0cf59")
 }
