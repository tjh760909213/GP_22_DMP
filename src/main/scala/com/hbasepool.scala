package com

import java.util.concurrent.Executors

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.executor.ExecutorService

object hbasepool {
  private val configValues = ConfigFactory.load("hbase.properties")
   var conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", configValues.getString("hbase.zookeeper.quorum"))
  conf.set("hbase.zookeeper.property.clientPort", configValues.getString("hbase.zookeeper.property.clientPort"))
  conf.set("hbase.defaults.for.version.skip", configValues.getString("hbase.zookeeper.property.clientPort"))
  private val service= Executors.newFixedThreadPool(5)
  val conn= ConnectionFactory.createConnection(conf, service)
}
