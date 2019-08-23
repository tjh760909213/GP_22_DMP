package com.ConnectionPool

import java.sql.{Connection, DriverManager}
import java.util.ResourceBundle

import scala.collection.mutable

object connectionPool {
  private val reader = ResourceBundle.getBundle("connection")
  private val max_connection = reader.getString("jeecg.max_connection") //连接池总数
  private val connection_num = reader.getString("jeecg.connection_num") //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new mutable.LinkedList[Connection]() //连接池
  private val driver = reader.getString("jeecg.driver")
  private val url = reader.getString("jeecg.url")
  private val username = reader.getString("jeecg.username")
  private val password = reader.getString("jeecg.password")
  /**
    * 加载驱动
    */
//  private def before() {
//    if (current_num > max_connection.toInt && pools.isEmpty {
//      print("busyness")
//      Thread.sleep(2000)
//      before()
//    } else {
//      Class.forName(driver)
//    }
//  }
//  /**
//    * 获得连接
//    */
//  private def initConn(): Connection = {
//    val conn = DriverManager.getConnection(url, username, password)
//    conn
//  }
////  /**
//  ////    * 初始化连接池
//  ////    */
//  ////  private def initConnectionPool(): mutable.LinkedList[Connection] = {
//  ////    AnyRef.synchronized({
//  ////      if (pools.isEmpty) {
//  ////        before()
//  ////        for (i <- 1 to connection_num.toInt) {
//  ////          pools.push(initConn())
//  ////          current_num += 1
//  ////        }
//  ////      }
//  ////      pools
//  ////    })
//  ////  }
//  ////  /**
//  ////    * 获得连接
//  ////    */
//  ////  def getConn():Connection={
//  ////    initConnectionPool()
//  ////    pools.poll()
//  ////  }
//  ////  /**
//  ////    * 释放连接
//  ////    */
//  ////  def releaseCon(con:Connection){
//  ////    pools.push(con)
//  ////  }


}
