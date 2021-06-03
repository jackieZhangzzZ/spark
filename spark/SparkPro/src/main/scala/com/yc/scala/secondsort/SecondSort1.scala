package com.yc.scala.secondsort

import org.apache.spark.{SparkConf, SparkContext}

object SecondSort1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("second sort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val productInfo = Array("apple 5 500","pear 2 500","banana 1 10000","grape 10 500")
    val productRDD = sc.parallelize(productInfo)
  }
}

case class ProductInfo(name:String,price:Double,anount:Int)