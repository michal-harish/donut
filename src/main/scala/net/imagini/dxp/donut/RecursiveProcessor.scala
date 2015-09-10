package net.imagini.dxp.donut

import kafka.consumer.SimpleConsumer

class RecursiveProcessor {

  val brokerHosts = (1 to 3).map(x => s"message-0${x}.prod.visualdna.com")
  val brokerPort = 9092
  val topic = "datasync"

  //highest common factor modulo of physical partition to get list of partitions
  //synchronized via zookeeper logical partition assignment (needs to handle changes)
  //val partitions = List() //get assigned partitions for the given topic

  def findSeedBroker: SimpleConsumer = {
    for (host <- brokerHosts) try {
      val consumer: SimpleConsumer = new SimpleConsumer(host, brokerPort, 100000, 64 * 1024, "leaderLookup")
      return consumer
//      try {
//        val topics = Seq(topic)
//        val req = new TopicMetadataRequest(topics, 0)
//      } finally {
//        consumer.close
//      }
    } catch {
      case e: Throwable => println(s"Problem occured while communicating with kafka admin api ${host}:${brokerPort} ", e.getMessage)
    }
    throw new Exception("Could not establish connection with any of the seed brokers")
  }


//  for (partition <- partitions) {
//
//    //load checkpointed state
//
//    //start simple consumer from the end of the checkpoint
//
//    //start CHM instance (which runs                 checkpoint thread
//
//    //start the processor thread (with it's own zk-based offset
//    new Thread {
//
//      start
//
//      override def run = {
//        //retrieve processor offset from zk
//        val stream: Iterator = null
//        while (stream.hasNext) {
//          val msg = stream.next
////          updateLocalState(msg)
//
////          if (msg.offset >= zk.offset) {
////            process(msg)
////            updateZkOffset(msg.offset)
////          }
//
//        }
//      }
//    }
//
//  }

  def handleLeaderChange: Unit = {

  }

  def handleRebalance: Unit = {

  }

}
