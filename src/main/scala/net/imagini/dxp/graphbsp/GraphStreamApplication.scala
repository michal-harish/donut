package net.imagini.dxp.graphbsp

import net.imagini.dxp.common.VdnaClusterConfig
import org.apache.donut.DonutApp
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamApplication(config: Configuration) extends DonutApp[GraphStreamProcessUnit](config) {
  def this() = this(new VdnaClusterConfig {
    set("kafka.group.id", "GraphStreamingBSP")
    set("kafka.topics", "graphstream,graphstate")
    setBoolean("kafka.cogroup", true)
    set("yarn.name", "GraphStreaming")
    set("yarn.queue", "developers")
    setBoolean("yarn.keepContainerst", true)
    setInt("yarn.master.priority", 0)
    setLong("yarn.master.timeout.s", 3600L)
  })

}
