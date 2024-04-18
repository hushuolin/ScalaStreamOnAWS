import org.apache.spark.sql.{SparkSession, functions => F}

object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Data Analysis")
      .master("local[*]") // 或者设置你的集群配置
      .getOrCreate()

    // 读取 CSV 文件
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // 自动推断字段类型
      .csv("src/main/resources/output/csvOutput/part-00000-c6c3fb30-b914-4214-9ac7-49f9e858fc9d-c000.csv")

    // 执行数据分析, 例如计算每个公司的投诉总数
    val analysisResult = df.groupBy("company")
      .agg(F.count("complaint_id").alias("total_complaints"))
      .orderBy(F.col("total_complaints").desc)

    // 展示结果（可选）
    analysisResult.show()

    // 将结果保存为新的 CSV 文件
    analysisResult.coalesce(1) // 如果结果不大，可以合并为一个文件
      .write
      .option("header", "true")
      .csv("src/main/resources/output/csvOutput/output_analysis.csv")

    spark.stop()
  }
}
