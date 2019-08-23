package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsProCity extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val province = row.getAs[String]("rtbprovince")
    val city = row.getAs[String]("rtbcity")
    if(StringUtils.isNotBlank(province)){
      list:+=("ZP"+province,1)
    }
    if(StringUtils.isNotBlank(province)){
      list:+=("ZC"+province,1)
    }

    list
  }
}
