package com.yc.projects.movieAnalysis


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
object Movie_RDD_3 {
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

   //所有电影中平均得分最高的前10部电影:（分数,电影ID）
    val ratings = ratingsRDD.map(_.split("::"))
        .map(x=>(x(0),x(1),x(2)))
    ratings.cache()
    println("平均得分最高的10部电影")

    println("简版的输出")
    ratings.map(x=>(x._2,(x._3.toDouble,1)))
        .reduceByKey(((x,y)=>{
          (x._1+y._1,x._2+y._2)
        }))
        .map(x=>(x._2._1/x._2._2,x._1))
        .sortByKey(false)
        .take(10)
        .foreach(println)


    println("按平均分取前10部电影输出详情:(movieid,title,genre,总评分,观影次数,平均分)")
    val moviesInfo = moviesRDD.map(_.split("::"))
        .map(movie=>{
          (movie(0),(movie(1),movie(2)))
        })

    val ratingsInfo = ratings.map(x=>(x._2,(x._3.toDouble,1)))
        .reduceByKey((x,y)=>{
          (x._1+y._1,x._2+y._2)
        })
        .map(x=>(x._1,(x._2._1/x._2._2,x._2._1,x._2._2)))

    moviesInfo.join(ratingsInfo)
        .map(info=>{
          (info._2._2._1,(info._1,info._2._1._1,info._2._1._2,info._2._2._2,info._2._2._3))
        })
        .sortByKey(false)
        .take(10)
        .foreach(println)

    println("观影人数最多的前10部电影")
    println("简版的输出(观影人数,电影编号)")
    ratings.map(x=>(x._2,1))
        .reduceByKey((x,y)=>{
          (x+y)
        })
        .map(x=>(x._2,x._1))
        .sortByKey(false)
        .take(10)
        .foreach(println)

    println("详情的输出(观影人数,电影编号)")
    moviesInfo.join(ratingsInfo)
        .map(info=>{
          (info._2._2._3,(info._1,info._2._1._2,info._2._2._2,info._2._2._1))
        })
        .sortByKey(false)
        .take(10)
        .foreach(println)
    sc.stop()

  }
}
