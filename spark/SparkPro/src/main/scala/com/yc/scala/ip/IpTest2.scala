package com.yc.scala.ip

import com.yc.IpRule.utils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 版本1:求access.log 每个省的访问次数
 */
object IpTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "C:\\Users\\12048\\Desktop/ip.txt"
      //读取规则
    val rules = utils.readRules(path)  //本地数据  driver

    //TODO:将rules改为广播变量
    val broadcastRef = sc.broadcast(rules)

    //access.log日志文件比较大,利用集群运算
    val lines = sc.textFile("C:\\Users\\12048\\Desktop/access.log")
    //解析lines中的每一行,取出ip,转成十进制,再去rules 中匹配到对应的地址(省)
    val provinceAndOne = lines.map(line => {
      val fields = line.split("\\|")             // |或
      val ip = fields(1)
      val ipNum = utils.ip2Long(ip)
//      //查找
//      val index:Int = utils.binarySearch(rules,ipNum)
      //TODO:从广播变量中取值
      val value = broadcastRef.value
      //TODO:查找
      val index = utils.binarySearch(value , ipNum)

      //取省名
      var province = "unknow"
      if(index != -1){
        val ipRule = rules(index)
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
