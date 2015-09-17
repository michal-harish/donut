package net.imagini.dxp.common

import java.io.FileInputStream

import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 15/09/15.
 */
class VdnaClusterConfig extends Configuration {
  addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/core-site.xml"))
  addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/hdfs-site.xml"))
  addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/yarn-site.xml"))
  set("yarn.classpath", "/opt/scala/scala-library-2.10.4.jar:/opt/scala/kafka_2.10-0.8.2.1.jar") //:/opt/scala/snappy-java-1.1.1.6.jar:/opt/scala/lz4-1.2.0.jar
  set("zookeeper.connect", "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com")
  set("kafka.brokers", "message-01.prod.visualdna.com:9092,message-02.prod.visualdna.com:9092,message-03.prod.visualdna.com:9092")
}
