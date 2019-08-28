package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.{TagUtils, redisPool}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  *上下文标签
  */
object TagsContext2 {
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



    val map: Map[String, String] = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/data/app_dict.txt" )
      .map ( _.split ( "\\t" ) ).filter ( _.size >= 5 ).map ( a => {
      (a ( 4 ), a ( 1 ))
    } ).collect ().toMap


    val stopmap: Array[String] = sc.textFile ( "/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/data/stopwords.txt" ).collect()

    val bmap: Broadcast[Map[String, String]] = sc.broadcast(map)
    val stopbro: Broadcast[Array[String]] = sc.broadcast(stopmap)
    //import spark.implicits._

    //读取数据
    val df = sQLContext.read.parquet(inputPath)

    //过滤符合Id的数据
    val baseRDD: RDD[(List[String], Row)] = df.filter ( TagUtils.OnesuserId ).rdd
      //所有的标签都在内部实现
      .map( row => {
        //取出用户id
        val userList: List[String] = TagUtils.getAllUserId ( row )
        (userList,row)
      })

    //构建点集合
    val verticesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap ( tp => {
      val jedis: Jedis = redisPool.getJedis ()
      val row = tp._2
      //所有标签
      val adList: List[(String, Int)] = TagsAd.makeTags ( row )
      val appList: List[(String, Int)] = TagsApp.makeTags ( row, bmap, jedis )
      val channelsList: List[(String, Int)] = Tagschannels.makeTags ( row )
      val euiqtList: List[(String, Int)] = TagsEquitment.makeTags ( row )
      val procityList: List[(String, Int)] = TagsProCity.makeTags ( row )
      val keywordsList = TagsKeywords.makeTags ( row, stopbro )
      val business = BusinessTag.makeTags ( row )
      val AllTag = adList ++ appList ++ channelsList ++ channelsList ++ euiqtList ++ procityList ++ keywordsList ++ business

      //List((String,Int))
      //保证其中一个点携带者所有标签，同时也保留所有userId
      val VD = tp._1.map ( (_, 0) ) ++ AllTag
      //处理所有的点集合
      tp._1.map ( uId => {
        //保证一个点携带标签 (uid,vd),后面全都是(uid,list())
        if (tp._1.head.equals ( uId )) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      } )
    } )


    //构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap ( tp => {
      //A B C : A->B  A->C
      tp._1.map ( uId => Edge ( tp._1.head.hashCode, uId.hashCode, 0 ) )
    } )

    //构建图
    val graph = Graph(verticesRDD,edges)
    //取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    //取出所有的标签和id
    vertices.join(verticesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      //聚合所有标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)

    sc.stop()



//    val res1 = res.reduceByKey((list1,list2)=>{
//      (list1:::list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_+_._2))
//        .toList
//    })


    //
    //    res1.map{
    //      case(userid,userTag)=>{
    //
    //        val put = new Put(Bytes.toBytes(userid))
    //        //处理下标签
    //        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
    //        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
    //        (new ImmutableBytesWritable(),put)
    //      }
    //    }.saveAsHadoopDataset(jobconf)







    //res.saveAsTextFile("/Users/denyo/Desktop/练习/第四阶段/Spark用户画像分析/data/biaoqian1")
    //res1.foreach(println)


  }

}