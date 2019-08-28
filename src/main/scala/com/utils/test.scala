package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf ().setAppName ( this.getClass.getName ).setMaster ( "local[*]" )
    val sc = new SparkContext ( conf )

    val list = List("116.310003,39.991957")
    val rdd: RDD[String] = sc.makeRDD(list)

    val bs: RDD[String] = rdd.map ( t => {
      val arr = t.split ( "," )
      AmapUtil.getBusinessFromAmap ( arr ( 0 ).toDouble, arr ( 1 ).toDouble )
    } )
    bs.foreach(println)
  }
}
