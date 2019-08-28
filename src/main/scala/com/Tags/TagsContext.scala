package com.Tags

import com.hbasepool
import com.typesafe.config.ConfigFactory
import com.utils.{TagUtils, redisPool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  *上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
//    if (args.length != 4) {
//      println ( "目录不匹配" )
//      sys.exit ()
//    }


    val Array ( inputPath, outputPath, days ) = args

    //创建上下文
    val conf = new SparkConf ().setAppName ( this.getClass.getName ).setMaster ( "local[*]" )
    val sc = new SparkContext ( conf )
    val sQLContext =new SQLContext ( sc )
//    val spark = SparkSession.builder().config(conf).getOrCreate()


    //todo 调用Habase API
    //加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    //创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.quorum"))
    configuration.set("hbase.zookeeper.property.clientPort", load.getString("hbase.zookeeper.property.clientPort"))

    //    configuration.set("hbase.zoopkeeper.quorum","10.211.55.101:2181")
    //configuration.set("hbase.master",load.getString("habse.master"))
    //创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    //判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
      val tableDescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    //创建JobConf
    val jobconf = new JobConf(configuration)
    //指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

//    //2
//    val hbadmin2: Admin = hbasepool.conn.getAdmin
//    val tableDescriptor2: HTableDescriptor = new HTableDescriptor("a")
//    val descriptor2 = new HColumnDescriptor("tags")
//    tableDescriptor2.addFamily(descriptor2)
//    //hbadmin2.createTable(tableDescriptor2)
//    val jobConf2 = new JobConf(hbasepool.conf)
//    //指定输出类型和表
//    jobConf2.setOutputFormat(classOf[TableOutputFormat])
//    jobConf2.set(TableOutputFormat.OUTPUT_TABLE,"a")



//    import spark.implicits
    val map: Map[String, String] = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/data/app_dict.txt" )
      .map ( _.split ( "\\t" ) ).filter ( _.size >= 5 ).map ( a => {
      (a ( 4 ), a ( 1 ))
    } ).collect ().toMap


    val stopmap: Array[String] = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/data/stopwords.txt" ).collect()

    val bmap: Broadcast[Map[String, String]] = sc.broadcast(map)
    val stopbro: Broadcast[Array[String]] = sc.broadcast(stopmap)


    //读取数据
    val df = sQLContext.read.parquet(inputPath)

    //过滤符合Id的数据
    val res: RDD[(String, List[(String, Int)])] = df.filter ( TagUtils.OnesuserId )
      //所有的标签都在内部实现
      .rdd.mapPartitions(part=>{

      val jedis = redisPool.getJedis()

      part.map( row => {
      val ID = row.getAs[String]("sessionid")
      //取出用户id
      val userId = TagUtils.getOneUserId ( row )
      //接下来通过row数据，打上所有标签(按照需求)
      val adList: List[(String, Int)] = TagsAd.makeTags ( row )
      val appList: List[(String, Int)] = TagsApp.makeTags ( row, bmap,jedis )
      val channelsList: List[(String, Int)] = Tagschannels.makeTags ( row )
      val euiqtList: List[(String, Int)] = TagsEquitment.makeTags ( row )
      val procityList: List[(String, Int)] = TagsProCity.makeTags ( row )
      val keywordsList = TagsKeywords.makeTags ( row,stopbro )
      val business = BusinessTag.makeTags(row)
      (ID,adList ++ appList ++ channelsList ++ channelsList ++ euiqtList ++ procityList ++ keywordsList ++ business)
    } )
    })

    val res1 = res.reduceByKey((list1,list2)=>{
      (list1:::list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    })



    res1.map{
      case(userid,userTag)=>{

        val put = new Put(Bytes.toBytes(userid))
        //处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobconf)







    //res.saveAsTextFile("/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/data/biaoqian1")
    res1.foreach(println)

    sc.stop()
  }

}
