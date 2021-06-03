package com.yc.JDBCrdd

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDDTest1 {
  val getConn = () =>{
    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","a")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jdbc rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val jdbcRDD = new JdbcRDD(sc,getConn,"select * from ipresult where id>=? and id<=?",1,6,2,row =>{
      (row.getInt(1),row.getString(2),row.getInt(3))
    })
    val result = jdbcRDD.collect()
    result.foreach(println)
    sc.stop()
  }
}
