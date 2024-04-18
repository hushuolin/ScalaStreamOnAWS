package dataAnalysis

import org.apache.spark.sql.SparkSession

object transformAndAnalysis {
  def main(args: Array[String]): Unit = {
    // 初始化 Spark 会话
    val spark = SparkSession.builder
      .appName("Convert Parquet to CSV")
      .config("spark.master", "local") // 可以更改为 "spark://master:port" 用于集群运行
      .getOrCreate()

    // 设定日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 读取 Parquet 文件
    val parquetFile = "src/main/resources/data_19601.parquet"
    val dataFrame = spark.read.parquet(parquetFile)

    // 数据处理，这里仅仅是简单地显示前几行以便调试
    dataFrame.show()

    // 将数据写入 CSV 文件
    val csvOutput = "src/main/resources/output/csvOutput"
    dataFrame.write
      .option("header", "true") // 在 CSV 文件中包含头部信息
      .mode("overwrite") // 添加这行代码以覆盖现有文件
      .csv(csvOutput)

    // 停止 Spark 会话
    spark.stop()
  }

}
