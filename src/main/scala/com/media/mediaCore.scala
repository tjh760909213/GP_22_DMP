package com.media

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{RptUtils, mediaUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object mediaCore {
  def main(args: Array[String]): Unit = {
    //创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val sQLContext: SQLContext = new SQLContext(sc)


    val datardd: RDD[String] =sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/app_dict.txt" )
    val broadcas: Broadcast[Array[String]] = sc.broadcast(datardd.collect())
    val idname: RDD[(String, String)] = sc.parallelize(broadcas.value).map(_.split("\t")).filter(_.size>5).map( a=>(a(4),a(1)))

    //获取数据
    val df = sQLContext.read.parquet(inputPath)


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val keylist: RDD[(String, List[Double])] = df.rdd.map ( row => {
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
      val appid = row.getAs [String]( "appid" )
      var appname = row.getAs[String]("appname")

      if (appname == ""){
        idname.map(a=>{
          if(a._1==appid){
            appname = a._2
          }
        })
      }
      //创建三个对应的方法处理九个指标
      val req = mediaUtils.request ( requestmode, processnode )
      val cli = mediaUtils.click ( requestmode, iseffective )
      val ad: List[Double] = mediaUtils.Ad ( iseffective, isbilling, isbid, iswin, adorderid, Winprice, adpayment )
      (appname, req ++ cli ++ ad)
    } )


    import spark.implicits._
    val res: RDD[(String, List[Double])] = keylist.reduceByKey( (a, b)=>a.zip(b).map( a=>a._1+a._2))

    val resframe: DataFrame = res.map ( a => (a._1, a._2 ( 0 ), a._2 ( 1 ), a._2 ( 2 ), a._2 ( 3 ), a._2 ( 4 ), a._2 ( 5 ), a._2 ( 6 ), a._2 ( 7 ), a._2 ( 8 )) )
      .toDF ( "appname", "req" ,"Vreq","Adrequest","bidding","biddingV","click","show","winprice","adpayment")



//    //加载配置文件 需要使用对应依赖包
//    val load: Config = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//
//    resframe.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop )


    spark.stop()

  }
}
