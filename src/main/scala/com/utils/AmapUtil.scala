package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * 商圈解析工具
  */
object AmapUtil {

  //获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={

    val location = long+","+lat
    val urlstr = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=2d24d3f8f2e10bca938db3886f690fc3"


    //调用请求
    val jsonstr: String = HttpUtil.get(urlstr)

    val jsonparse: JSONObject = JSON.parseObject(jsonstr)
    //判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""

    //接下来解析内部json串，判断每个key的json串不能为空
    val regeocodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson==null || regeocodeJson.keySet().isEmpty) return ""


    val addressComponentJson: JSONObject = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray =  addressComponentJson.getJSONArray("businessAreas")
    if (businessAreasArray == null || businessAreasArray.isEmpty) return  null
    //创建集合 保存数据
    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    //循环输出
    for(item <- businessAreasArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append((json.getString("name")))
      }
    }

    buffer.mkString(",")
  }


}
