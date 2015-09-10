package net.imagini.dxp.donut

import net.imagini.dxp.common.{Edge, Vid}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by mharis on 10/09/15.
 */
class GraphMessageTest extends FlatSpec with Matchers {

  behavior of "GraphMessage"
  it should "correctly serialize and deserialize payloads and keys" in {
    val k = Vid("vdna", "ffffffff-ffff-ffff-ffff-ffffffffffff")
    val edges = Map(Vid("a", "1") -> Edge("AAT", 1.0, 1000L))
    val msg = GraphMessage(k, 5, edges)
    val bytes = msg.payload.map(_ & 0xff)
    bytes should be (Seq(
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

    msg.decodeKey should be(k)
    msg.decodePayload should be((5,edges))
  }

}
