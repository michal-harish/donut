package net.imagini.dxp.common

import java.nio.ByteBuffer

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 10/09/15.
 */
class BSPMessageTest extends FlatSpec with Matchers {

  behavior of "GraphMessage"
  it should "correctly serialize and deserialize payloads and keys" in {
    val k = Vid("vdna", "ffffffff-ffff-ffff-ffff-ffffffffffff")
    BSPMessage.decodeKey(BSPMessage.encodeKey(k)) should be(k)

    val edges = Map(Vid("a", "1") -> Edge("AAT", 1.0, 1000L))
    val bytes = BSPMessage.encodePayload((5, edges)).array
    bytes.map(_ & 0xff) should be (Seq(
      5, //iteration
      0,1,  //number of edges
        0,0,0,0,0,0,3,232, // ts
        //edge target vid:
        14, 0,0,0,0,  0,97, 0,0,0,0,0,0,0,2,
        //edge structure:
          1,    //version
          255,  //probability 1/x
          0,250 //vendor
    ))

    BSPMessage.decodePayload(bytes) should be((5,edges))
  }

}
