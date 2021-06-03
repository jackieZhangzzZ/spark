package com.yc.scala.teacherAnalysis

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计  各科目下  各页面访问的次数   按降序排序
 */
object Test1_subjectAndTeacherAnalysis {
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
      (page , 1)
    })
    //聚合
    val rdd3 = rdd2.reduceByKey(_+_)
    //根据上面元组结果的第二个元素来排序
    val rdd4 = rdd3.sortBy(_._2,false)

    val result = rdd4.collect()
    result.foreach(println)
    sc.stop()
  }
}
