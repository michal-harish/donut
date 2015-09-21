package net.imagini.dxp.syncstransform

import java.io.FileInputStream
import java.util.Properties

import org.apache.donut.DonutApp

/**
 * Created by mharis on 15/09/15.
 *
 * This is a simple transformation streaming processor. Each unit (SyncsTransformProcessUnit) processes fixed
 * set of partitions from json serialized 'datasync' topic and transforms each sync (a pair of connected IDs)
 * to a pair of messages representing a delta edge and reverse edge between the IDs into 'graphstream' topic.
 */
class GraphSyncsStreamingTransform(config: Properties) extends DonutApp[SyncsTransformProcessUnit](config) {
  def this() = this(new Properties {
    /**
     * pipeline environment global configuration
     * yarn1.site=/etc/...
     * yarn1.queue=...
     * yarn1.classpath=/opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar
     * kafka.brokers=...
     * zookeeper.connect=...
     */
    load(new FileInputStream("/etc/vdna/graphstream/config.properties"))

    /**
     *  GraphSyncsStreamingTransform component configuration
     */
    setProperty("yarn1.keepContainers", "true")
    setProperty("kafka.group.id", "GraphSyncsStreamingBSP")
    setProperty("kafka.topics", "datasync")
    setProperty("kafka.cogroup", "false")
  })

}
