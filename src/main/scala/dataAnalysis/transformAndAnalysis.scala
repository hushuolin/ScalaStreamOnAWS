package dataAnalysis

import org.apache.spark.sql.SparkSession

object transformAndAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Convert Parquet to CSV")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val parquetFile = "src/main/resources/data_19601.parquet"
    val dataFrame = spark.read.parquet(parquetFile)

    dataFrame.show()

    val csvOutput = "src/main/resources/output/csvOutput"
    dataFrame.write
      .option("header", "true")
      .mode("overwrite")
      .csv(csvOutput)

    spark.stop()
  }

}
