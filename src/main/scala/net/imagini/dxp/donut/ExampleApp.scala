package net.imagini.dxp.donut

import org.apache.donut.DonutProducer

/**
 * Created by mharis on 10/09/15.
 */
object ExampleApp extends App {

  val zkHosts = "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com"
  val brokers = "message-01.prod.visualdna.com:9092,message-02.prod.visualdna.com:9092,message-03.prod.visualdna.com:9092"

  val intervention = new Object
  val producer = DonutProducer[GraphMessage](brokers)
  val transformer = new SyncsTransformer(zkHosts, brokers, numThreads = 4, producer)
  val debugger = new Debugger(zkHosts, producer, numThreads = 4)
  //val processor = new RecursiveProcessor

  @volatile var running = true
  try {
    debugger.start
    transformer.start
    //processor.run
    while(running) {
      intervention.synchronized( intervention.wait(1000) )
      println(debugger.counter.get)
    }
  } finally {
    //processor.close
    transformer.stop
    debugger.stop
  }
}
