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
object Movie_RDD_2 {
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

    //某个用户看过的电影数量
   val userWhatedMovie = ratingsRDD.map(_.split("::"))
       .map(item => {(item(1),item(0))})
       .filter(_._2.equals(userId))
    println(userId+"观看过的电影数:"+userWhatedMovie.count())

    println("这些电影的详情:\n")
    //需求二:这些电影的信息,格式为:(MovieID,Title,Genres)
    val movieInfoRDD = moviesRDD.map(_.split("::"))
        .map(movie => {(movie(0),(movie(1),movie(2)))})
    val result = userWhatedMovie.join(movieInfoRDD)
        .map(item=>{(item._1,item._2._1,item._2._2._1,item._2._2._2)})
    result.foreach(println)
    sc.stop()

  }
}
