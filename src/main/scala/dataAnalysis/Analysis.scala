import org.apache.spark.sql.{SparkSession, functions => F}

object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Data Analysis")
      .master("local[*]") // 或者设置你的集群配置
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // 自动推断字段类型
      .csv("src/main/resources/output/csvOutput/part-00000-c6c3fb30-b914-4214-9ac7-49f9e858fc9d-c000.csv")

    val analysisResult = df.groupBy("company")
      .agg(F.count("complaint_id").alias("total_complaints"))
      .orderBy(F.col("total_complaints").desc)

    analysisResult.show()

    analysisResult.coalesce(1) // 如果结果不大，可以合并为一个文件
      .write
      .option("header", "true")
      .csv("src/main/resources/output/csvOutput/output_analysis.csv")

    spark.stop()
  }
}
