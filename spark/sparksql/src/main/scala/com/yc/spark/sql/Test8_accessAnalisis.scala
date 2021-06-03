package com.yc.spark.sql

import com.yc.spark.utils.YcUtil
import org.apache.spark.sql.SparkSession

object Test8_accessAnalisis {
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
      (startNum,endNum,province)
    })
    val ipDataFrame = ipDataset.toDF("startNum","endNum","province")
    //ipDataFrame.show()

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
    ipDataFrame.createTempView("v_ip")
    accessIpDataFrame.createTempView("v_accessIpLong")
   // val resultDataFrame = spark.sql("select province, count(*) cn from v_accessIpLong inner join v_ip on (ipNum>=startNum and ipNum<=endNum) group by province ")
   //方案二:API+DSL
   val resultDataFrame = ipDataFrame.join(accessIpDataFrame,$"ipNum">=$"startNum" and $"ipNum"<=$"endNum").groupBy("province").count()
    resultDataFrame.show()
    spark.stop()
  }
}
