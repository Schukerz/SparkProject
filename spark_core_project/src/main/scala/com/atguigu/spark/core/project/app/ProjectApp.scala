package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    //获取数据
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("projectApp")
    val sc: SparkContext = new SparkContext(conf)
    val sourceRDD: RDD[String] = sc.textFile("X:\\spark\\user_visit_action.txt")
    val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
      val splits: Array[String] = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong
      )
    })


    //需求1:Top10 热门品类 Map(category_id,点击量,下单量,支付量)
//    val categoryCountInfo: Array[CategoryCountInfo] = UserVisitActionTop10App.userVisitActionTop10(sc,userVisitActionRDD)

    //需求2:Top10热门品类中每个品类的 Top10 活跃 Session 统计
    //Map(cid,iterable(sid,count))
//    CategorySessionidTop10App.sessionidTop10(userVisitActionRDD,categoryCountInfo)

    //需求3:页面单跳转化率统计
    PageConversionApp.pageConversion(sc,userVisitActionRDD,"1,2,3,4,5,6,7")

  }
}
