import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.col
object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Data Analysis")
      .master("local[*]") // 或者设置你的集群配置
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // 自动推断字段类型
      .csv("src/main/resources/output/csvOutput/part-00000-977de863-f78c-40b8-9944-6dcc2b9993f0-c000.csv")

    val analysisResult = df.groupBy("company")
      .agg(F.count("complaint_id").alias("total_complaints"))
      .orderBy(F.col("total_complaints").desc)

    analysisResult.show()

    analysisResult.coalesce(1) // 如果结果不大，可以合并为一个文件
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/resources/AnalysisOutput")

    spark.stop()
  }

}
