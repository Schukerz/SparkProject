package com.atguigu.spark.sql.project.app

import org.apache.spark.sql.SparkSession

object SqlDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SqlDemo")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use sparkpractice")
    spark.sql(
      """
        |select ci.*,
        |pi.product_name
        |from user_visit_action uv
        |join product_info pi on uv.click_product_id = pi.product_id
        |join city_info ci on uv.city_id = ci.city_id
      """.stripMargin).createOrReplaceTempView("t1")


    //注意这里使用自定义函数remarK的时候,不用把city_name放入group by里面.remark本身就是一个聚合函数
    spark.udf.register("remark",new Remark)
      spark.sql(
        """
          |select area,
          |product_name,
          |remark(city_name) remark,
          |count(*) ct
          |from t1
          |group by area,product_name
        """.stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select *,
        |rank() over(partition by area order by ct desc) rk
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")
    val df = spark.sql(
      """
        |select area,
        |product_name,
        |ct,
        |remark
        |from t3
        |where rk <=3
      """.stripMargin)
    df.coalesce(1).write.mode("overwrite").saveAsTable("area_city_count")
//    df.write.mode("overwrite").json("X:\\spark")
  }

}
