package com.yc.IpRule

import com.yc.java.IpRule

import scala.io.{BufferedSource, Source}
/*
  单机查找
 */
object utils {
  def main(args: Array[String]): Unit = {
    //测试ip2Long
    val ipNum = ip2Long("1.0.1.0")
    println(ipNum)
    val ipRules = readRules("C:\\Users\\12048\\Desktop/ip.txt")
    println(ipNum)
    val index = binarySearch(ipRules,ipNum)
    println(index)
  }
  def ip2Long(ip:String):Long={
    val fragments = ip.split("\\.")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L       // |   <<优先级
    }
    ipNum
  }

  def readRules(path:String):Array[IpRule]={
    val bf:BufferedSource = Source.fromFile(path)
    val lines:Iterator[String] = bf.getLines()
    // Scala ： map 单机
    //spark  ： 集群
    val rules:Array[IpRule] = lines.map(line =>{
      val fields = line.split("\\|")
      //TODO: 目前只解析了  startIP,endIP,province,其他的可以自己加入
      val startIpLong = fields(2).toLong
      val endIpLong = fields(3).toLong
      val province = fields(6)
      new IpRule(startIpLong,endIpLong,province)
    }).toArray
    rules
  }

  /**
   * 到给定的ip规则(ipRules列表) 中查找  ip ,返回下标
   * @param ipRules
   * @param ip
   * @return
   */
  def binarySearch(ipRules:Array[IpRule],ip:Long):Int={
    var low = 0
    var high = ipRules.length -1
    while(low<=high){
      var middle = (low+high) / 2
      if((ip>=ipRules(middle).getStartIpInLong)&&(ip<=ipRules(middle).getEndIpInLong)){
        return middle
      }else if(ip < ipRules(middle).getStartIpInLong){
        high = middle - 1
      }else{
        low = middle + 1
      }
    }
    -1
  }
}
