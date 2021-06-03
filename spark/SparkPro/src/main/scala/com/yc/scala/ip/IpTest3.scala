package com.yc.scala.ip

import com.yc.IpRule.utils
import com.yc.java.IpRule
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 版本1:求access.log 每个省的访问次数
 */
object IpTest3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\12048\\Desktop/ip.txt"
      //读取规则
    //val rules = utils.readRules(path)  //本地数据  driver

    //TODO:使用spark集群来完成读取
    val rulesLines = sc.textFile(path)  //这时数据是一个RDD,而不是一个Array
    val ipRulesRdd = rulesLines.map(line => {
      //一行ip规则,按|切分,取第3，4，省份列,包装成 IpRule
      val fields = line.split("\\|")
      val startIpLong = fields(2).toLong
      val endIpLong = fields(3).toLong
      val province = fields(6)
      new IpRule(startIpLong,endIpLong,province)
    })
    //广播变量的使用规则
    //1.不能将RDD使用广播变量广播出去,因为RDD是不存储数据的.但可以将RDD的结果广播出去
    val rulesInDriver = ipRulesRdd.collect()   //collect 将executor的运算结果汇总到driver中

    val broadcastRef = sc.broadcast(rulesInDriver)

    //access.log日志文件比较大,利用集群运算
    val lines = sc.textFile("C:\\Users\\12048\\Desktop/access.log")
    //解析lines中的每一行,取出ip,转成十进制,再去rules 中匹配到对应的地址(省)
    val provinceAndOne = lines.map(line => {
      val fields = line.split("\\|")             // |或
      val ip = fields(1)
      val ipNum = utils.ip2Long(ip)
      //      //查找
      val value = broadcastRef.value

      val index:Int = utils.binarySearch(value , ipNum)
      //取省名
      var province = "unknow"
      if(index != -1){
        val ipRule = rulesInDriver(index)
        province=ipRule.getProvince()
      }
      (province,1)
    })
    //再归约
    val reduce = provinceAndOne.reduceByKey(_+_)
    reduce.foreach(println)
    sc.stop()
  }
}
