package dataAnalysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TrendAnalysis {

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder
      .appName("Complaints Trend Analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Set the Spark SQL configuration to LEGACY to allow parsing of the old timestamp format
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    // Read the parquet file into a DataFrame
    val parquetFile = "src/main/resources/data_19601.parquet"
    val df = spark.read.parquet(parquetFile)

    // Convert date_received to date type
    val complaints = df.withColumn("date_received", to_date(col("date_received"), "yyyy-MM-dd'T'HH:mm:ss"))

    // Aggregate data by date to see number of complaints per day
    val complaintsByDay = complaints.groupBy("date_received").count()
      .orderBy("date_received")

    // Show results
    complaintsByDay.show()

    // Save results
    complaintsByDay.write.format("csv").save("src/main/resources/complaints_trend_by_day.csv")

    spark.stop()
  }
}
