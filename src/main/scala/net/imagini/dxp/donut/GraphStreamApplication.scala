package net.imagini.dxp.donut

import java.io.FileInputStream
import java.util.concurrent.Executors

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

  YarnClient.submitApplicationMaster(conf, 0, "developers", false, classOf[GraphStreamApplicationMaster], args)
}

class GraphStreamApplicationMaster extends YarnMaster {

  protected override def onStartUp(args: Array[String]) {
    requestContainerGroup(12, GraphStreamContainer.getClass, args, 0, 10 * 1024, 1)
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
      val transformer = new SyncsTransformer(zkHosts, producer, "r", "d", "a")
      val processor = new RescursiveProcessorHighLevel(zkHosts, producer)

      @volatile var running = true
      val executor = Executors.newFixedThreadPool(1)
      try {
        executor.submit(processor)
        executor.submit(transformer)
        while (running) {
          signal.synchronized(signal.wait(600000))
          println("num.recursive.messages = " + processor.counter.get + ", state.size = " + processor.state.size)
        }
      } finally {
        executor.shutdownNow();
        producer.close
      }
    } catch {
      case e: Throwable => {
        e.printStackTrace(System.out)
        //TODO Alert
      }
    }
  }

}
