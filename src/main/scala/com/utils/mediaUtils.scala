package com.utils

object mediaUtils {
  //此方法处理请求数
  def request(requestmode:Int,processnode:Int):List[Double]={
    var f = 0
    var s = 0
    var t = 0
    if(requestmode==1) {
      if(processnode == 3){
        f+=1
        t+=1
        s+=1
      }else if(processnode == 2){
        f+=1
        s+=1
      }else{
        f+=1
      }
    }
    List(f,s,t)
  }

  //此方法处理展示点击数
  def click(requestmode:Int,iseffective:Int):List[Double]={
    var show = 0
    var click = 0
    if(iseffective==1){
      if(requestmode ==2){
        show+=1
      }else if(requestmode == 3){
        click+=1
      }
    }
    List(show,click)
  }

  //此方法处理竞价操作
  def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,
         adorderid:Int,Winprice:Double,adpayment:Double):List[Double]={
    var bidding = 0
    var biddingV = 0
    var cons: Double = 0
    var cost: Double = 0
    if(iseffective==1 && isbilling == 1){
      bidding+=1
      if(iswin==1){
        biddingV+=1
        cons+=Winprice/1000
        cost+=adpayment/1000
      }
    }
    if (iseffective==1 && isbid == 1){
      bidding+=1
    }

    List(bidding,biddingV,cons,cost)
  }


}
