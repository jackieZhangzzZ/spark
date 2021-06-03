package com.yc.scala.teacherAnalysis

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求二   统计  科目各页面访问的次数  按降序排序
 *
 * 假设:数据集非常大  无法一次性在内存中进行排序    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse)
 * 解决方案:过滤  只对一部分数据排序
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    var path = "db/visit.log"
    if(args.length>=1){
      path = args(0)
    }

    val conf = new SparkConf().setAppName("teacher analysis").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //读文件  visit.log
    val lines = sc.textFile(path)
    val rdd2 = lines.map(line => {
      val index = line.lastIndexOf("/")
      val page = line.substring(index+1)

      val httpHost = line.substring(0,index)
      val subject = new URL(httpHost).getHost().split("\\.")(0)
      ((subject,page),1)
    })

    //聚合
    val rdd3 = rdd2.reduceByKey(_+_)
    //根据上面元组结果的第二个元素来排序
    val subjects = Array("bigdata","javaee","front")
    for(sb <- subjects){
      val filtered = rdd3.filter(_._1._1==sb)
      val t = filtered.sortBy(_._2,false).take(4)
      println("科目:"+sb)
      t.foreach(println)
    }
    sc.stop()
  }
}
