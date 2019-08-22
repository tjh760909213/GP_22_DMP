package com.ETL


import com.utils.{SchemaUtils, Utils2Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object txt2Parquet {
  def main(args: Array[String]): Unit = {
    //System.setProperties("hadoop.home.dir","")

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

    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 进行数据的读取，处理分析数据
    val lines: RDD[String] = sc.textFile(inputPath)
    // 按要求切割，并且保证数据的长度大于等于85个字段，
    // 如果切割的时候遇到相同切割条件重复的情况下，需要切割的话，那么后面需要加上对应匹配参数
    // 这样切割才会准确 比如 ,,,,,,, 会当成一个字符切割 需要加上对应的匹配参数
    val rowRDD: RDD[Row] = lines.map( t=>t.split(",",t.length)).filter(_.length >= 85)
      .map(arr=>{
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type.toInt(arr(73)),
          Utils2Type.toDouble(arr(74)),
          Utils2Type.toDouble(arr(75)),
          Utils2Type.toDouble(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type.toInt(arr(84))
        )
      })

    //构建DF
    val df = sQLContext.createDataFrame(rowRDD,SchemaUtils.structtype)

    //保存数据
    df.write.parquet(outputPath)
    sc.stop()

//    val df: DataFrame = spark.createDataFrame(rowRDD,SchemaUtils.structtype)
//
//    val selected: DataFrame = df.select("provincename","cityname")
//
//    val RowRdd: RDD[Row] = selected.rdd
//
//    val rdd: RDD[((String, String), Int)] = RowRdd.map( a=>(a(0).toString,a(1).toString)).map( a=>((a._1,a._2),1))
//
//    val res: RDD[(String, String, Int)] = rdd.groupBy(_._1).map( a=>(a._1._1,a._1._2,a._2.size))
//
//
//    import spark.implicits._
//
//    val resF: DataFrame = res.toDF("provincename","cityname","rt")
//    resF.write.format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8")
//      .option("dbtable", "local")
//      .option("user", "root")
//      .option("password", "123456")
//      .save()
//
//
//    res.saveAsTextFile(outputPath)


  }
}



/**
val cityList: RDD[(String, List[(String, String)])] = rdd.groupBy(_._2).mapValues(_.toList)
    val list: RDD[List[(String, String)]] = cityList.map(_._2)

    val res: RDD[List[(Int, String, String)]] = list.map ( a => {
      val tuples: List[(Int, String, String)] = a.map ( b => {
        val cityname = b._2
        val provincename = b._1
        val rt = a.size
        (rt, provincename, cityname)
      } )
      tuples.distinct
    } )
  **/