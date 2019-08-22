package com.ProCityCt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * 需求指标二：统计地域各省市分布情况
  */
object proCity {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if(args.length != 2){
      println("目标参数不正确，退出程序")
      sys.exit()
    }

    //创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc: SparkContext = new SparkContext(conf)
    val sQLContext: SQLContext = new SQLContext(sc)

    //设置SparkSession类
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    //读取本地文件
    val df: DataFrame = spark.read.parquet(inputPath)
    //val df = sQLContext.read.parquet(inputPath)
    //注册临时表
    df.createTempView("log")
    //指标统计
    val result: DataFrame = sQLContext.sql("select count(*),provincename,cityname from log group by provincename,cityname")
//    result.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)

    //加载配置文件 需要使用对应依赖包
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    result.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop )

    sc.stop()


  }
}
