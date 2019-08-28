package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object redisCon {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(conf)
    val data: Map[String, String] = sc.textFile("/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/app_dict.txt")
      .map ( _.split ( "\\t" ) ).filter ( _.size >= 5 ).map ( a => {
      (a ( 4 ), a ( 1 ))
    } ).collect().toMap
    //println(data)

    val data1= sc.textFile("/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/app_dict.txt").collect().toBuffer.toString()


    val jedis = redisPool.getJedis()

    jedis.set("l",data1)

//    val str: Array[String] = jedis.get("l").split(",")


//    data.map(a=>{
//
//      jedis.set(a._1,a._2)
//
//    })
    //val str: String = jedis.get("app_dict")


    jedis.close()
    sc.stop()
  }
}
