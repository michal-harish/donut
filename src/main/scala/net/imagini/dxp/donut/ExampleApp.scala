package net.imagini.dxp.donut

import java.io.FileInputStream

import org.apache.donut.DonutProducer
import org.apache.hadoop.conf.Configuration
import org.apache.yarn1.YarnMaster

/**
 * Created by mharis on 10/09/15.
 */
object ExampleApp extends App {
  val conf: Configuration = new Configuration

  conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/core-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/stag/etc/hadoop/yarn-site.xml"))
  conf.set("master.queue", "developers")
  conf.setInt("master.priority", 0)
  conf.setLong("master.timeout.s", 600L)

  YarnMaster.submitApplicationMaster(conf, classOf[ExampleMaster], args)
}

class ExampleMaster extends YarnMaster {

  override def allocateContainers {
    println("ALLOCATION")
    val numPartitions = 12 //TODO find a common partitioning factor
    for(p <- (1 to numPartitions)) {
      requestContainer(0, ExampleWorker.getClass, 1024, 1)
    }
  }
}

object ExampleWorker {

  def main(args: Array[String]) : Unit = {
    val zkHosts = "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com"
    val brokers = "message-01.prod.visualdna.com:9092,message-02.prod.visualdna.com:9092,message-03.prod.visualdna.com:9092"

    val signal = new Object
    val producer = DonutProducer[GraphMessage](brokers)
    val transformer = new SyncsTransformer(zkHosts, brokers, numThreads = 1, producer)
    val debugger = new Debugger(zkHosts, producer, numThreads = 1)
    //val processor = new RecursiveProcessor

    @volatile var running = true
    try {
      debugger.start
      transformer.start
      //processor.run
      while (running) {
        signal.synchronized(signal.wait(10000))
        println(debugger.counter.get)
      }
    } finally {
      //processor.close
      transformer.stop
      debugger.stop
    }
  }

}
