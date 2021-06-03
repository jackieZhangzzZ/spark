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
object Movie_RDD_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    var appName = "movie analysis"

    if(args.length>0){
      masterUrl = args(0)
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

    occupationsRDD.cache()
    usersRDD.cache()
    ratingsRDD.cache()
    moviesRDD.cache()

    println("职业数:"+occupationsRDD.count())
    println("电影数:"+moviesRDD.count())
    println("用户数:"+usersRDD.count())
    println("评分条数:"+ratingsRDD.count())

    val usersBasic = usersRDD.map(_.split("::"))
      .map(user => (user(3),(user(1),user(2),user(4)))
    )
    val occupations = occupationsRDD.map(_.split("::"))
      .map(occupation =>{
        (occupation(0),occupation(1))
      })

    //合并
    val usersInfo = usersBasic.join(occupations)
    println("用户详情(格式:(职业编号,(人的编号,性别,年龄,邮编),职业名)):")
    usersInfo.foreach(println)
    println("合并后共有:"+usersInfo.count()+"条users记录")
    sc.stop()

  }
}
