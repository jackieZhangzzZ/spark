package com.yc.spark.sql

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test2_SQLContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark sql one").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val lines=sc.textFile("data/person.txt")
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
    val df = sqlContext.createDataFrame(personRDD,schema)

    df.registerTempTable("t_person")
    val resultDataFrame = sqlContext.sql("select * from t_person order by age desc,height desc,name desc")
    resultDataFrame.show()

    sc.stop()
  }
}

