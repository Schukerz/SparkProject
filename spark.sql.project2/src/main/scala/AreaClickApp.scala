import org.apache.spark.sql.SparkSession

object AreaClickApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AreaClickApp")
      .enableHiveSupport()
      .getOrCreate()
    println("ini")
    spark.sql("show databases").show()
    spark.sql("use sparkpractice")
    println("t1")
    spark.sql(
      """
        |select ci.*,
        |pi.product_name
        |from user_visit_action uv
        |join product_info pi on uv.click_product_id = pi.product_id
        |join city_info ci on uv.city_id = ci.city_id
      """.stripMargin).createOrReplaceTempView("t1")
println("t2")
    //注册函数
    spark.udf.register("remark",new AreaClickUDAF)
    spark.sql(
      """
        |select area,
        |product_name,
        |count(*) ct,
        |remark(city_name) remark
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")
println("t3")
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
        |where rk <= 3
      """.stripMargin)
    df.show(1000,false)
    df.coalesce(1).write.mode("overwrite").saveAsTable("area_city_count")
    spark.close()
  }

}
