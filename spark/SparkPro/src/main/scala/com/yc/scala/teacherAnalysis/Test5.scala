package com.yc.scala.teacherAnalysis

import java.net.URL

import org.apache.spark.{Partitioner,SparkConf, SparkContext}

import  scala.collection.mutable
/**
 * 需求二   统计  科目各页面访问的次数  按降序排序
 *
 * 假设:数据集非常大  无法一次性在内存中进行排序    单一科目下的数据也无法在内存完成排序
 * 解决方案:利用Executor 来完成排序  另外要将数据(分组聚合后的数据 ) 放到不同的分区
 */
object Test5 {
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
    //val rdd3 = rdd2.reduceByKey(_+_)

    val subjects = rdd2.map(_._1._1).distinct().collect()

    //创建分区器
    val sbPartitioner = new SubjectPartitioner1(subjects)

    val rdd3 = rdd2.reduceByKey(sbPartitioner,_+_)

    val result = rdd3.mapPartitions( it => {
      it.toList.sortBy(_._2).reverse.take(5).iterator
    })
    result.saveAsTextFile("C:\\Users\\12048\\Desktop/result")
    sc.stop()
  }
}

class SubjectPartitioner1(subjects:Array[String]) extends Partitioner{

  val map = new mutable.HashMap[String,Int]()
  var i = 0
  for(s <- subjects){
    map.put(s,i)
    i+=1
  }
  override def numPartitions: Int = {
    subjects.length
  }

  override def getPartition(key: Any): Int = {
    val subjectname = key.asInstanceOf[(String,String)]._1
    map(subjectname)
  }
}
