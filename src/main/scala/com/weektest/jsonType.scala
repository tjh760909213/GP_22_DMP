package com.weektest

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object jsonType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val spark =SparkSession.builder().config(conf).getOrCreate()

    val strarr: mutable.Buffer[String] = sc.textFile("/Users/denyo/Desktop/练习/第四阶段/项目阶段周考/第一次/json.txt").collect().toBuffer
    var list: List[(String, Int)] = List[(String,Int)]()

    for(jsonstr <- strarr){
      val jsonparse: JSONObject = JSON.parseObject(jsonstr)
      val status = jsonparse.getIntValue("status")
      if(status == 1){
        val regeocodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
        if(regeocodeJson != null){
          val array: JSONArray = regeocodeJson.getJSONArray("pois")
          for(item <- array.toArray()){
            val json = item.asInstanceOf[JSONObject]
            val `type`: Array[String] = json.getString("type").split(";")
            for(a <- `type`){
              list:+=(a,1)
            }
          }
        }
      }

      val stringToInt: Map[String, Int] = list.groupBy ( _._1 ).map ( a => {
        (a._1, a._2.size)
      } )
      stringToInt.foreach(println)

    }
  }
}
