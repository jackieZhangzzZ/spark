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
 * under 18: 1
 * 18 - 24: 18
 * 25 - 34: 25
 * 35 - 44: 35
 * 45 - 49: 45
 * 50 - 55: 50
 * 56 + 56
 */
object Movie_RDD_5 {
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

    var age = "1"
    println("年龄段为:"+age+"喜爱的电影top10:")
    //筛选人  age = 1
    val userWithAge = usersRDD.map(_.split("::"))
        .map(x => (x(0),x(2)))
        .filter(_._2.equals(age))
    userWithAge.cache()
    println("年龄为1的用户量:"+userWithAge.count())

    val ratings = ratingsRDD.map(_.split("::"))
        .map(x=>{
          (x(0),(x(1),x(2)))
        })
    ratings.cache()
    println("评分数据量为:"+ratings.count())

    //join
    var ratingWithAge = userWithAge.join(ratings)
        .map(item=>(item._2._2._1,item._2._1,item._1,item._2._2._2))
    println("年龄在1的评分数据量为:"+ratingWithAge.count())

    //计算平均分
    ratingWithAge.map(x=>(x._1,(x._4.toDouble,1)))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .map(item => (item._2._1/item._2._2,(item._1,item._2._1,item._2._2)))
        .sortByKey(false)
        .take(10)
        .foreach(println)

    //扩展:上面的显示中加入电影信息    电影名,类型
    println("年龄段为:"+age+"喜爱的电影top10:(movieID,电影名,平均分,总分数,观影次数,类型)")
    val rdd = ratingWithAge.map(x=>(x._1,(x._4.toDouble,1)))
      .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
      .map(item => (item._1,(item._2._1/item._2._2,item._2._1,item._2._2)))

    val movieInfos = moviesRDD.map(x=>x.split("::"))
        .map(x=>(x(0),(x(1),x(2))))

    val joinedRdd = rdd.join(movieInfos)
    joinedRdd.cache()

    joinedRdd.map(x=>(x._2._1._1,(x._1,x._2._2._1,x._2._1._2,x._2._1._3,x._2._2._2)))
        .sortByKey(false)
        .map(x=>(x._2._1,x._2._2.substring(0,x._2._2.indexOf("(")),x._1,x._2._3,x._2._4,x._2._5))
        .take(10)
        .foreach(println)

    //扩展二:二次排序,先根据平均分排序，如平均分相同，按观影次数排序(降)，观影次数相同，按电影名排序(升)
    println("二次排序,先根据平均分排序，如平均分相同，按观影次数排序(降)，观影次数相同，按电影名排序(升)")
    joinedRdd.sortBy(item=>(-item._2._1._1,-item._2._1._3,item._2._2._1))
        .map(x=>(x._1,x._2._2._1,x._2._2._2,x._2._1._1,x._2._1._2,x._2._1._3))
        .take(10)
        .foreach(println)

    //扩展三:是否可以利用广播变量完成操作
    println("年龄在:"+age+"的用户数据量:"+userWithAge.count())
    val userArray = userWithAge.collect()
    val broadcastRef = sc.broadcast(userArray)
    val ageRef = sc.broadcast(age)

    val ratingWithAgeRdd = ratings.filter(rate=>{
      val userArray = broadcastRef.value
      val age = ageRef.value
      userArray.contains((rate._1,age))
    })

//    println("用户的打分数据量有:"+ratingWithAgeRdd.count())
//    ratingWithAgeRdd.map(item=>(item._2._1,(item._2._2.toDouble,1)))
//        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
//        .map(item=>item._1,item._2._1,item._2._2,item._2._1/item._2._2)
//        .sortBy(item=>(-item._4,-item._3,item._1))
//        .take(100)
//        .foreach(println)

    sc.stop()

  }
}
