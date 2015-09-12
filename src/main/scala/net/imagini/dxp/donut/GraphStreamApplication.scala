package net.imagini.dxp.donut

import java.io.FileInputStream

import org.apache.donut.DonutProducer
import org.apache.hadoop.conf.Configuration
import org.apache.yarn1.{YarnClient, YarnMaster}

/**
 * Created by mharis on 10/09/15.
 */
object GraphStreamApplication extends App {
  val conf: Configuration = new Configuration

  conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/core-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/yarn-site.xml"))
  conf.set("classpath", "/opt/scala-2.10.4/lib/scala-library.jar")
  conf.set("master.queue", "developers")
  conf.setInt("master.priority", 0)
  conf.setLong("master.timeout.s", 3600L)

  YarnClient.submitApplicationMaster(conf, 0, "developers", classOf[GraphStreamApplicationMaster], args)
}

class GraphStreamApplicationMaster extends YarnMaster {

  protected override def onStartUp(args: Array[String]) {
    requestContainerGroup(12, GraphStreamContainer.getClass, Array("1"), 0, 10 * 1024, 1)
  }
  protected override def onCompletion(): Unit = {

  }
}

object GraphStreamContainer {

  def main(args: Array[String]): Unit = {
    try {
      val zkHosts = "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com"
      val brokers = "message-01.prod.visualdna.com:9092,message-02.prod.visualdna.com:9092,message-03.prod.visualdna.com:9092"

      val signal = new Object

      val producer = DonutProducer[GraphMessage](brokers)
      val transformer = new SyncsTransformer(zkHosts, brokers, numThreads = args(0).toInt, producer)
      val processor = new RescursiveProcessorHighLevel(zkHosts, producer, numThreads = 1)

      @volatile var running = true
      try {
        processor.start
        transformer.start
        while (running) {
          signal.synchronized(signal.wait(10000))
          println(processor.counter.get)
        }
      } finally {
        transformer.stop
        processor.stop
      }
    } catch {
      case e: Throwable => e.printStackTrace(System.out)
        Thread.sleep(10000)
    }
  }

}
