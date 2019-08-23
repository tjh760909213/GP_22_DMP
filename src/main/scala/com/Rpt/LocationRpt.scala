package com.Rpt

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

import util._
/**
  * 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
//    if(args.length != 2){
//      println("目标参数不正确，退出程序")
//      sys.exit()
//    }

    //创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val sQLContext: SQLContext = new SQLContext(sc)

    //获取数据
    val df = sQLContext.read.parquet(inputPath)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._


    //将数据进行处理，统计各个指标
    val r: RDD[((String, String), List[Double])] = df.rdd.map( row => {
      //把需要的字段全部取到
      val requestmode = row.getAs [Int]( "requestmode" )
      val processnode = row.getAs [Int]( "processnode" )
      val iseffective = row.getAs [Int]( "iseffective" )
      val isbilling = row.getAs [Int]( "isbilling" )
      val isbid = row.getAs [Int]( "isbid" )
      val iswin = row.getAs [Int]( "iswin" )
      val adorderid = row.getAs [Int]( "adorderid" )
      val Winprice = row.getAs [Double]( "winprice" )
      val adpayment = row.getAs [Double]( "adpayment" )
      //key值 是地域的省市
      val pro = row.getAs [String]( "provincename" )
      val city = row.getAs [String]( "cityname" )

      //创建三个对应的方法处理九个指标
      val req= RptUtils.request ( requestmode, processnode )
      val cli = RptUtils.click ( requestmode, iseffective )
      val ad: List[Double] = RptUtils.Ad ( iseffective, isbilling, isbid, iswin, adorderid, Winprice, adpayment )

      ((pro, city), req ++ cli ++ ad)
    } )

    val res: RDD[((String, String), List[Double])] = r.reduceByKey ( (a, b) => {
      List ( a ( 0 ) + b ( 0 ), a ( 1 ) + b ( 1 ), a ( 2 ) + b ( 2 ), a ( 3 ) + b ( 3 ), a ( 4 ) + b ( 4 ), a ( 5 ) + b ( 5 ), a ( 6 ) + b ( 6 ), a ( 7 ) + b ( 7 ), a ( 8 ) + b ( 8 ) )
    } )




    res.saveAsTextFile(outputPath)



  }
}
