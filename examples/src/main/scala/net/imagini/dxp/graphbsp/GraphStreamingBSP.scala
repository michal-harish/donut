package net.imagini.dxp.graphbsp

import java.io.FileInputStream
import java.util.Properties

import org.apache.donut.DonutApp

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
class GraphStreamingBSP(config: Properties) extends DonutApp[GraphStreamingBSPProcessUnit](config) {
  def this() = this(new Properties {
    /**
     * pipeline environment global configuration
     * yarn1.site=/etc/...
     * yarn1.queue=...
     * yarn1.classpath=/opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar
     * zookeeper.connect=...
     * kafka.brokers=...
     */

    load(new FileInputStream("/etc/vdna/graphstream/config.properties"))

    /**
     *  GraphStreamingBSP component configuration
     */
    setProperty("yarn1.keepContainers", "true")
    setProperty("kafka.group.id", "GraphStreamingBSP")
    setProperty("kafka.topics", "graphstream,graphstate")
    setProperty("kafka.cogroup", "true")
  })

}
