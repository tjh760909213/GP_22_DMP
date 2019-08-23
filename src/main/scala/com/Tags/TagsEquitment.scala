package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row


object TagsEquitment extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val os = row.getAs[Integer]("client")
    os match {
      case v if v ==1 => list:+=("Andiord D00010001",1)
      case v if v ==2 => list:+=("Ios D00010002",1)
      case v if v ==3 => list:+=("wp D00010003",1)
      case v => list:+=("其他 D00010004",1)
    }

    val network = row.getAs[Integer]("networkmannerid")
    network match {
      case v if v ==1 =>list:+=("WIFI D00020001",1)
      case v if v ==2 =>list:+=("4G D00020002",1)
      case v if v ==3 =>list:+=("3G D00020003",1)
      case v if v ==4 =>list:+=("2G D00020004",1)
      case v  =>list:+=("其他 D00020005",1)
    }

    val ispname = row.getAs[Int]("ispid")
    ispname match  {
      case v if v == 1 =>list:+=("移动 D00030001",1)
      case v if v == 2 =>list:+=("联通 D00030002",1)
      case v if v == 3 =>list:+=("电信 D00030003",1)
      case v =>list:+=("其他",1)
    }

    list
  }
}
