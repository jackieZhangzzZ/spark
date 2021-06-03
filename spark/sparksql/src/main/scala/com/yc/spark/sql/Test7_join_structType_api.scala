package com.yc.spark.sql

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Test7_join_structType_api {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val classesLinesDataset=spark.sparkContext.textFile("data/classes.txt")
    val studentsLinesDataset=spark.sparkContext.textFile("data/students.txt")

    val classesRDD = classesLinesDataset.map(line=>{
      val fields = line.split(" ")
      val cid = fields(0).toInt
      val cname = fields(1)
      Row(cid,cname)
    })
    val studentsRDD = studentsLinesDataset.map(line=>{
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val height = fields(3).toDouble
      val cid = fields(4).toInt
      Row(id,name,age,height,cid)
    })

    val schema = StructType(List(
      StructField("cid",IntegerType,true),
      StructField("cname",StringType,true)
    ))

    val schema2 = StructType(List(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("height",DoubleType,true),
      StructField("classid",IntegerType,true)
    ))
    val classesDataframe = spark.createDataFrame(classesRDD,schema)
    val studentDataframe = spark.createDataFrame(studentsRDD,schema2)

    val resultDataFrame = classesDataframe.joinWith(studentDataframe,$"cid"===$"classid","left")
    resultDataFrame.show()

    spark.stop()
  }
}
