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
object Movie_RDD_4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[2]"
    //var userId = "18"

    var appName = "movie analysis"

    if(args.length>0){
      masterUrl = args(0)
      //userId =args(1)
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

    //1.分析男性用户最喜欢的前10部电影
    val ratings = ratingsRDD.map(_.split("::"))
        .map(x=>(x(0),(x(1),x(2))))
    ratings.cache()
    val users = usersRDD.map(_.split("::"))
        .map(x=>(x(0),x(1)))
    users.cache()

    //将ratings   users合并
    val ratingwithGender = ratings.join(users)
        .map(item=>(item._1,item._2._1._1,item._2._1._2,item._2._2))

    //取出女性打分(UserID,MovieID,Rating,Gender)
    val femaleRatings = ratingwithGender.filter(item=>item.equals("F"))
    femaleRatings.cache()
    //取出男性打分(UserID,MovieID,Rating,Gender)
    val maleRatings  = ratingwithGender.filter(item=>item.equals("M"))
    maleRatings.cache()

    println("女性评分总分最高的10部电影,格式:总评分,movieID")
    femaleRatings.map(x=>(x._2,x._3.toDouble))
        .reduceByKey((x,y)=>x+y)
        .map(item=>(item._2,item._1))
        .sortByKey(false)
        .take(10)
        .foreach(println)

    println("女性评分总分最高的10部电影,格式:总评分,movieID")
    maleRatings.map(x=>(x._2,x._3.toDouble))
      .reduceByKey((x,y)=>x+y)
      .map(item=>(item._2,item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)


    //改进版:累计总分后计算平均分，按平均分排序,显示top10,显示电影名
    val femaleRatingsDetail = femaleRatings.map(x=>(x._2,(x._3.toDouble,1)))
        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
        .map(x=>(x._1,(x._2._1,x._2._2,x._2._1/x._2._2)))

    val maleRatingsDetail = maleRatings.map(x=>(x._2,(x._3.toDouble,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(x=>(x._1,(x._2._1,x._2._2,x._2._1/x._2._2)))

    val movieInfos = moviesRDD.map(x => x.split("::"))
        .map(x=>(x(0),(x(1),x(2))))

    println("女性评分平均分最高的10部电影详情")
    femaleRatingsDetail.join(movieInfos)
        .map(x=>(x._2._1._3,(x._1,x._2._1._1,x._2._1._2,x._2._2._1,x._2._2._2)))
        .sortByKey(false)
        .take(10)
        .foreach(println)

    println("男性评分平均分最高的10部电影详情")
    maleRatingsDetail.join(movieInfos)
      .map(x=>(x._2._1._3,(x._1,x._2._1._1,x._2._1._2,x._2._2._1,x._2._2._2)))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    sc.stop()

  }
}
