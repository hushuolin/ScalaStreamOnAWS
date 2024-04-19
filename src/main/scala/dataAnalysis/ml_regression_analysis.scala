package dataAnalysis
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.datediff
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.regression.LinearRegression

object ml_regression_analysis {

  def mlMethod(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Analysis")
      .master("local[*]")
      .getOrCreate()

    // need to change the path if generated new csv
    val df = spark.read
      .option("header", "true")
      .csv("src/main/resources/output/csvOutput/part-00000-977de863-f78c-40b8-9944-6dcc2b9993f0-c000.csv")

    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    // 文本处理
    val tokenizer = new Tokenizer().setInputCol("complaint_what_happened").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1000)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    // 使用KMeans算法
    val kmeans = new KMeans().setK(5).setSeed(1)

    val clusteringPipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, kmeans))

    // 应用模型
    val model = clusteringPipeline.fit(df)
    val clusterPredictions = model.transform(df)
    clusterPredictions.select("complaint_what_happened", "prediction").show(5)

  }

}
