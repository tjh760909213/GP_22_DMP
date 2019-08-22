package com.Rpt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object LocationRptSQl {
  def main(args: Array[String]): Unit = {
    //创建一个集合保存输入和输出目录
    val Array ( inputPath , outputPath) = args
    val conf = new SparkConf ().setAppName ( this.getClass.getName ).setMaster ( "local[*]" )
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set ( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )

    //创建执行入口
    val sc: SparkContext = new SparkContext ( conf )
    val sQLContext: SQLContext = new SQLContext ( sc )

    val spark = SparkSession.builder ().config ( conf ).getOrCreate ()
    val data: DataFrame = spark.read.parquet(inputPath).select("requestmode","processnode","iseffective","isbilling",
    "isbid","iswin","winprice","adpayment","provincename","cityname" )

    data.createTempView("log")


    val filt: DataFrame = sQLContext.sql ( "select provincename,cityname, " +
      "case when requestmode = 1 and processnode >= 1 then 1 else 0 end as req ," +
      "case when requestmode = 1 and processnode >= 2 then 1 else 0 end as Vreq , " +
      "case when requestmode = 1 and processnode >= 3 then 1 else 0 end as adreq, " +
      "case when iseffective = 1 then 1 else 0  end as bidding ," +
      "case when iseffective = 1 and isbilling = 1 and iswin = 1 then  1 else 0 end as biddingV, " +
      "case when requestmode = 2 and iseffective = 1 then 1 else 0 end as click,  " +
      "case when requestmode = 3 and iseffective = 1 then 1 else 0 end as show," +
      "case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice else 0 end as winprice, " +
      "case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment else 0  end as adpayment  from log " )
    filt.createTempView("fil")

    val res = spark.sql("select provincename,cityname,sum(req),sum(Vreq),sum(adreq),sum(bidding),sum(biddingV),sum(click),sum(show),sum(winprice)/1000,sum(adpayment)/1000  from fil group by provincename,cityname")

    //加载配置文件 需要使用对应依赖包
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop )

    spark.stop()



  }
}