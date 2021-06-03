package com.yc.spark.sql

import com.yc.spark.utils.YcUtil
import org.apache.spark.sql.SparkSession

object Test9_accessAnalisis_broadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ip analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ipLinesDataset = spark.read.textFile("data/ip.txt")  //Dataset
    val ipDataset = ipLinesDataset.map(line=>{
      val fields = line.split("\\|")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6).toString
      IpRule(startNum,endNum,province)
    })
    val ipDataFrame = ipDataset.toDF("startNum","endNum","province")

    //发布成广播变量
    val ipRulesArray = ipDataFrame.collect()
    val broadcastRef = spark.sparkContext.broadcast(ipRulesArray)


    val accessDataset = spark.read.textFile("data/access.log")
    val accessIpDataFrame = accessDataset.map(line=>{
      val fields = line.split("\\|")
      val ip = fields(1)
      val ipNum = YcUtil.ip2Long(ip)
      ipNum
    }).toDF("ipNum")

    //accessIpDataFrame.show()
    //方案一:SQL方式
    //1.临时表或视图
    accessIpDataFrame.createTempView("v_accessIpLong")

    //自定义函数:sql  自定义函数
    spark.udf.register("ip2province",(ipNum:Long)=>{
      val ipRulesArray = broadcastRef.value
      var province = "unknow"
      var index = YcUtil.binarySearch(ipRulesArray,ipNum)
      if(index != -1){
        province = ipRulesArray(index).getAs("province")
      }
      province
    })

    val resultDataFrame = spark.sql("select ip2province(ipNum) province,count(*) cn from v_accessIpLong group by province")
    resultDataFrame.show()
    spark.stop()
  }
}
case class IpRule(startNum:Long,endNum:Long,province:String)
