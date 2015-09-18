package net.imagini.dxp.graphbsp

import net.imagini.dxp.common.VdnaClusterConfig
import org.apache.donut.DonutApp
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 14/09/15.
 *
 * This is a stateful recursive streaming processor. Each unit (GraphStreamProcessUnit) processes cogrouped partitions
 * from 2 topics, one for Delta and one for State:
 *
 * A. the Delta is recursively processed from and to topic 'graphstream'
 * B. the State is kept in a compacted topic 'graphstate'
 *
 * The input into this application comes from SyncsTransformApplication which provides fresh edges into the graph.
 * The input is amplified by recursive consulation of State and production of secondary delta messages.
 */
class GraphStreamApplication(config: Configuration) extends DonutApp[GraphStreamProcessUnit](config) {
  def this() = this(new VdnaClusterConfig {
    set("yarn.name", "GraphStreamingBSP")
    set("yarn.queue", "developers")
    setBoolean("yarn.keepContainers", true)
    setInt("yarn.master.priority", 0)
    setLong("yarn.master.timeout.s", 3600L)
    set("kafka.group.id", "GraphStreamingBSP")
    set("kafka.topics", "graphstream,graphstate")
    setBoolean("kafka.cogroup", true)
  })

}
