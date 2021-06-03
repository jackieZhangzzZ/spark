package com.yc.spark.utils

import com.yc.spark.sql.IpRule
import org.apache.spark.sql.Row

object YcUtil {
  def ip2Long(ip:String):Long={
    val fragments = ip.split("\\.")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L       // |   <<优先级
    }
    ipNum
  }

  def binarySearch(ipRules:Array[Row],ip:Long):Int={
    var low = 0
    var high = ipRules.length -1
    while(low<=high){
      var middle = (low+high) / 2
      if((ip>=ipRules(middle).getAs("startNum").asInstanceOf[Long])&&(ip<=ipRules(middle).getAs("endNum").asInstanceOf[Long])){
        return middle
      }else if(ip < ipRules(middle).getAs("startNum").asInstanceOf[Long]){
        high = middle - 1
      }else{
        low = middle + 1
      }
    }
    -1
  }
}
