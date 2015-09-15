package net.imagini.dxp.graphbsp

import net.imagini.dxp.common.DXPConfig
import org.apache.donut.DonutApp

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamApplication extends DonutApp[GraphStreamProcessUnit](3 * 1024, false, "graphstream") {
  val config = new DXPConfig {
    set("kafka.group.id", "GraphStreamingBSP")
    set("yarn.name", "GraphStreamingBSP")
    set("yarn.queue", "developers")
    setInt("yarn.master.priority", 0)
    setLong("yarn.master.timeout.s", 3600L)
  }

  def runOnYarn: Unit = runOnYarn(config)

  def runLocally: Unit = runLocally(config)
}
