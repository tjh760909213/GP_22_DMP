package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tagschannels extends  Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val channelId= row.getAs[Integer]("adplatformproviderid")

   // if(StringUtils.isNoneBlank(channelId)){
      list:+=("CN"+channelId,1)
    //}

    list
  }
}
