package com.atguigu.spark.sql.project.app

import org.apache.spark.sql.SparkSession

object SelectTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Select").enableHiveSupport().getOrCreate()
    spark.sql("use sparkpractice")
    spark.sql("select * from area_city_count").show(1000)
      spark.close()

  }
}
