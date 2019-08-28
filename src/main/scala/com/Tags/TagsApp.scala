package com.Tags

import com.utils.{Tag, redisPool}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.Jedis

/**
  * App标签
  */
object TagsApp extends Tag{



  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    val broadcast= args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val jedis = args(2).asInstanceOf[Jedis]

    val idName: Map[String, String] = broadcast.value

    val appId: String = row.getAs[String]("appid")
    var appName: String = row.getAs[String]("appname")

   // val jedis = new Jedis("10.211.55.101",6379)




    if (appName == ""){
        //appName= idName.getOrElse(appId,"")
        jedis.get(appId)

    }
    list:+=("APP"+appName,1)

    list
  }
}
