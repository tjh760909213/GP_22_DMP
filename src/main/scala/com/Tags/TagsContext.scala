package com.Tags

import com.utils.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
//    if (args.length != 4) {
//      println ( "目录不匹配" )
//      sys.exit ()
//    }


    val Array ( inputPath, outputPath ) = args

    //创建上下文
    val conf = new SparkConf ().setAppName ( this.getClass.getName ).setMaster ( "local[*]" )
    val sc = new SparkContext ( conf )
    val sQLContext = new SQLContext ( sc )
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits
    val map: Map[String, String] = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/app_dict.txt" )
      .map ( _.split ( "\\t" ) ).filter ( _.size >= 5 ).map ( a => {
      (a ( 4 ), a ( 1 ))
    } ).collect ().toMap

    val stopmap: Array[String] = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/stopwords.txt" ).collect()

    val bmap: Broadcast[Map[String, String]] = sc.broadcast(map)
    val stopbro: Broadcast[Array[String]] = sc.broadcast(stopmap)


    //读取数据
    val df = sQLContext.read.parquet(inputPath)

    //过滤符合Id的数据
    val res: RDD[(String, List[(String, Int)])] = df.filter ( TagUtils.OnesuserId )
      //所有的标签都在内部实现
      .rdd.map ( row => {
      val ID = row.getAs[String]("sessionid")
      //取出用户id
      val userId = TagUtils.getOneUserId ( row )
      //接下来通过row数据，打上所有标签(按照需求)
      val adList: List[(String, Int)] = TagsAd.makeTags ( row )
      val appList: List[(String, Int)] = TagsApp.makeTags ( row, bmap )
      val channelsList: List[(String, Int)] = Tagschannels.makeTags ( row )
      val euiqtList: List[(String, Int)] = TagsEquitment.makeTags ( row )
      val procityList: List[(String, Int)] = TagsProCity.makeTags ( row )
      val keywordsList = TagsKeywords.makeTags ( row,stopbro )
      (ID,adList ++ appList ++ channelsList ++ channelsList ++ euiqtList ++ procityList ++ keywordsList)
    } )
    res.foreach(println)


    sc.stop()
  }

}
