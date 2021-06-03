package com.yc.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Test3_SQLContext_api {
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
      Person(id,name,age,height)
    })
    import sqlContext.implicits._
    val df = personRDD.toDF()
    //df.show()

//    df.registerTempTable("t_person")
//    val resultDataFrame = sqlContext.sql("select * from t_person order by age desc,height desc,name desc")
    val resultDataFrame = df.select("id","name","age","height").orderBy($"age" desc,$"height" desc,$"name" desc)

    resultDataFrame.show()

    sc.stop()
  }
}

//case class Person(id:Integer,name:String,age:Integer,height:Double)


