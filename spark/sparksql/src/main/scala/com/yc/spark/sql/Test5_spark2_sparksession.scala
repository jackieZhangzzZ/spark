package com.yc.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Test5_spark2_sparksession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    //val lines:Dataset[String]=spark.read.textFile("data/person.txt")  // Dataset
    val lines=spark.sparkContext.textFile("data/person.txt")  //RDD

    val personRDD=lines.map(line=>{
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val height = fields(3).toDouble

      Row(id,name,age,height)
    })
//    import sqlContext.implicits._
//    val df = personRDD.toDF()
//    df.show()
    val schema = StructType(List(
         StructField("id",IntegerType,true),
         StructField("name",StringType,true),
         StructField("age",IntegerType,true),
         StructField("height",DoubleType,true)
    ))

    val df = spark.createDataFrame(personRDD,schema)
    //val df = sqlContext.createDataFrame(personRDD,schema)

//    df.registerTempTable("t_person")
//    val resultDataFrame = spark.sql("select * from t_person order by age desc,height desc,name desc")
    import spark.implicits._
    //val resultDataFrame = df.select("id","name","age","height").orderBy($"age" desc,$"height" desc,$"name" desc)
    val resultDataFrame = df.select("id","name","age","height").where($"age">10)
            .orderBy($"age" asc,$"height" asc,$"name" asc)
    resultDataFrame.show()

    spark.stop()
  }
}

