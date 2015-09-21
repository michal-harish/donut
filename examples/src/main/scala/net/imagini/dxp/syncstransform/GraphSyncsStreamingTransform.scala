package net.imagini.dxp.syncstransform

import org.apache.donut.DonutApp
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 15/09/15.
 *
 * This is a simple transformation streaming processor. Each unit (SyncsTransformProcessUnit) processes fixed
 * set of partitions from json serialized 'datasync' topic and transforms each sync (a pair of connected IDs)
 * to a pair of messages representing a delta edge and reverse edge between the IDs into 'graphstream' topic.
 */
class GraphSyncsStreamingTransform(config: Configuration) extends DonutApp[SyncsTransformProcessUnit](config) {
  def this() = this(new Configuration {
    /**
     * pipeline environment global configuration
     * yarn1.site=/etc/...
     * yarn1.queue=...
     * yarn1.classpath=/opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar
     * kafka.brokers=...
     * zookeeper.connect=...
     */
    addResource("/etc/vdna/graphstream/config.properties")

    /**
     *  GraphSyncsStreamingTransform component configuration
     */
    setBoolean("yarn1.keepContainers", true)
    set("kafka.group.id", "GraphSyncsStreamingBSP")
    set("kafka.topics", "datasync")
    setBoolean("kafka.cogroup", false)
  })

}
