package com.yc.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}

object Test6_spark2_wordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val lines:Dataset[String]=spark.read.textFile("data/wc.txt")  // Dataset
    //val lines=spark.sparkContext.textFile("data/person.txt")  //RDD
    import spark.implicits._
    val words = lines.flatMap(_.split(" "))       // Dataset[String]

    //方案一sql:
//    words.createTempView("wc")
//    val result = spark.sql("select value,count(*) nums from wc group by value")
//    result.show()
    //方案二Api:
    val result = words.groupBy($"value" as "word").count().sort($"count" desc)
    result.show()
  }
}
