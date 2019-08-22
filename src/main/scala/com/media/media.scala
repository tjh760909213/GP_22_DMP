package com.media

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object media {
  def main(args: Array[String]): Unit = {
    //创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val sQLContext: SQLContext = new SQLContext(sc)

    //获取数据
    val df = sQLContext.read.parquet(inputPath).select("ispid","requestmode","iseffective","putinmodeltype","isbid","isbilling",
      "iswin","winprice","adpayment","processnode","ispid","networkmannername","devicetype","appid","appname")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    df.createTempView("log")

    import  spark.implicits._

//    val appid: DataFrame = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/app_dict.txt" ).map ( _.split ( "\\t" ) ).filter(_.size >=5).map ( a => {
//      (a ( 4 ), a ( 1 ))
//    } ).toDF ( "id", "name" )



    val datardd: RDD[String] =sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/app_dict.txt" )
    val broadcas: Broadcast[Array[String]] = sc.broadcast(datardd.collect())

    val appid: DataFrame = sc.parallelize(broadcas.value).map ( _.split ( "\\t" ) ).filter ( _.size >= 5 ).map ( a => {
      (a ( 4 ), a ( 1 ))
    } ).toDF ( "id", "name" )



    appid.createTempView("appid1")


    val stge: DataFrame = spark.sql ( "select appid, " +
      "case when requestmode = 1 and processnode >= 1 then 1 else 0 end as req ," +
      "case when requestmode = 1 and processnode >= 2 then 1 else 0 end as Vreq ," +
      "case when requestmode = 1 and processnode =3 then 1 else 0 end Adrequest," +
      "case when iseffective = 1 then 1 else 0  end as bidding ," +
      "case when iseffective = 1 and isbilling = 1 and iswin = 1 then  1 else 0 end as biddingV," +
      "case when requestmode = 2 and iseffective = 1 then 1 else 0 end as click," +
      "case when requestmode = 3 and iseffective = 1 then 1 else 0 end as show," +
      "case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end as winprice," +
      "case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0  end as adpayment  from log" )
    stge.createTempView("stage")

    spark.sql("select appid,sum(req) req,sum(Vreq) Vreq,sum(Adrequest) Adrequest,sum(bidding) bidding,sum(biddingV) biddingV,sum(click) click,sum(show) show,sum(winprice)/1000 winprice,sum(adpayment)/1000 adpayment from stage group by appid").createTempView("reshou")

    val res = spark.sql("select appid1.name,req ,Vreq,Adrequest,bidding,biddingV,click,show,winprice,adpayment from reshou join appid1 on reshou.appid = appid1.id ")

    //加载配置文件 需要使用对应依赖包
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop )

    spark.stop()



  }
}
