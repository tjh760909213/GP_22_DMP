package com.weektest

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object jsonAn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val spark =SparkSession.builder().config(conf).getOrCreate()

    val strarr: mutable.Buffer[String] = sc.textFile("/Users/denyo/Desktop/练习/第四阶段/项目阶段周考/第一次/json.txt").collect().toBuffer
    var list: List[(String, String)] = List[(String,String)]()



    for(jsonstr <- strarr){
      val jsonparse: JSONObject = JSON.parseObject(jsonstr)
      val status = jsonparse.getIntValue("status")
      if(status == 1){
        val regeocodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
        if(regeocodeJson != null){
          val array: JSONArray = regeocodeJson.getJSONArray("pois")
          for(item <- array.toArray()){
              val json = item.asInstanceOf[JSONObject]
              val id: String = json.getString("id")
              val businessarea = json.getString("businessarea")
              list:+=(id,businessarea)
          }
        }
      }
    }

    //list.foreach(println)

    val groupbyBus: Map[String, List[(String, String)]] = list.groupBy(_._1)
    groupbyBus.foreach(println)


    val tuples: List[(String, Int)] = list.map ( a => {
      val bus = a._2
      (bus, 1)
    } )
    val sumbus: Map[String, Int] = tuples.groupBy ( _._1 ).map ( a => {
      (a._1, a._2.size)
    } )
    sumbus.foreach(println)
  }
}
