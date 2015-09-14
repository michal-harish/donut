package net.imagini.dxp.donut

import java.io.FileInputStream

import org.apache.donut.DonutApp
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamApplication extends DonutApp[GraphStreamProcessUnit](false, "graphstream") {
  val conf: Configuration = new Configuration

  conf.set("yarn.name", "GraphStream")
  conf.set("yarn.queue", "developers")
  conf.setInt("yarn.master.priority", 0)
  conf.setLong("yarn.master.timeout.s", 3600L)
  conf.addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/core-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/yarn-site.xml"))
  conf.set("yarn.classpath", "/opt/scala-2.10.4/lib/scala-library.jar")
  conf.set("donut.zookeeper.connect", "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com")
  conf.set("donut.kafka.brokers", "message-01.prod.visualdna.com:9092,message-02.prod.visualdna.com:9092,message-03.prod.visualdna.com:9092")
  conf.set("donut.kafka.port", "9092")

  def runOnYarn: Unit = runOnYarn(conf)

  def runLocally: Unit = runLocally(conf)
}
