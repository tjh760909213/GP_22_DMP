package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeywords extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row =args(0).asInstanceOf[Row]
    val broad =args(1).asInstanceOf[Broadcast[Array[String]]].value

    val keywords: String = row.getAs[String]("keywords")

    var i:Int = 0
    if(keywords.contains("|")){
      val wordsArr: Array[String] = keywords.split("\\|")

      if (i<wordsArr.length){
        if(wordsArr(i).size>=3 && wordsArr(i).size<=8 && !broad.contains(wordsArr(i)) )
        {
          list:+=(wordsArr(i),1)
        }
        i+=1
      }
    }
    if(keywords.size>=3 && keywords.size<=8 && !broad.contains(keywords))
    {
      list:+=(keywords,1)
    }

    list
  }
}
