package net.imagini.dxp.graphbsp

import net.imagini.dxp.common.DXPConfig
import org.apache.donut.DonutApp
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamApplication(config: Configuration) extends DonutApp[GraphStreamProcessUnit](config) {
  def this() = this(new DXPConfig {
    set("kafka.group.id", "GraphStreamingBSP")
    set("kafka.topics", "graphstream,graphstate")
    setBoolean("kafka.cogroup", true)
    set("yarn.name", "GraphStreamingBSP")
    set("yarn.queue", "developers")
    setInt("yarn.master.priority", 0)
    setLong("yarn.master.timeout.s", 3600L)
  })

  def runOnYarn: Unit = runOnYarn(3 * 1024)

  def runLocally: Unit = runLocally(false)
}
