package com.yc.projects.movieAnalysis

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 需求一:
 *        1.读取信息,统计数据条数  职业数  电影数  用户数  评分数
 *        2.显示每个职业下的用户详细信息   显示为:(职业号,(人的编号,性别,年龄,邮编))
 */

/**
 * 电影点评系统用户行为分析:用户观看电影和点评电影的所有行为数据的采集、过滤、处理和展示:
 * 数据描述:
 * 1，"ratings.dat"：UserID::MovieID::Rating::Timestamp
 * *  2，"users.dat"：UserID::Gender::Age::OccupationID::Zip-code
 * *  3，"movies.dat"：MovieID::Title::Genres
 * *  4, "occupations.dat"：OccupationID::OccupationName
 */
object Movie_RDD_7 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    var userId = "18"

    var appName = "movie analysis"

    if(args.length>0){
      masterUrl = args(0)
      userId =args(1)
    }else if(args.length>1){
      appName = args(1)
    }
    //创建上下文
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    val sc = new SparkContext(conf)
    val filepath = "data/"
    val usersRDD = sc.textFile(filepath+"users.dat")
    val occupationsRDD = sc.textFile(filepath+"occupations.dat")
    val ratingsRDD = sc.textFile(filepath+"ratings.dat")
    val moviesRDD = sc.textFile(filepath+"movies.dat")

    //需求7:分析 年度电影数量
    //输出格式
    moviesRDD.map(x=>x.split("::"))
        .map(x=>(x(1),1))
        .map(item=>{      //(year,1)
          var mname = ""
          var year = ""
          val pattern = Pattern.compile("(.*) (\\(\\d{4}\\))")
          val matcher = pattern.matcher(item._1)
          if(matcher.find()){
            mname = matcher.group(1)
            year = matcher.group(2)
            year = year.substring(1,year.length()-1)
          }
          if(year == ""){
            (-1,1)
          }else{
            (year.toInt,1)
          }
        })
        .reduceByKey((x,y)=>x+y)
        .sortByKey()
        .foreach(println)

    sc.stop()

  }
}
