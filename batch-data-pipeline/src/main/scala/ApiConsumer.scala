import sttp.client3._
import ujson._
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import scala.util.{Try, Success, Failure}
import S3Uploader.uploadFileToS3


object ApiConsumer {
  val config = ConfigFactory.load()

  // Configurations loaded from file
  val apiEndpoint = config.getString("api.endpoint")
  val productName = config.getString("api.productName")
  val startDate = config.getString("api.startDate")
  val endDate = config.getString("api.endDate")
  val size = config.getInt("api.size")
  val sparkMaster = config.getString("spark.master")
  val sparkAppName = config.getString("spark.appName")

  case class Complaint(
                        company: Option[String],
                        company_public_response: Option[String],
                        company_response: Option[String],
                        complaint_id: Option[String],
                        complaint_what_happened: Option[String],
                        consumer_consent_provided: Option[String],
                        consumer_disputed: Option[String],
                        date_received: Option[String],
                        date_sent_to_company: Option[String],
                        has_narrative: Option[Boolean],
                        issue: Option[String],
                        product: Option[String],
                        state: Option[String],
                        sub_issue: Option[String],
                        sub_product: Option[String],
                        submitted_via: Option[String],
                        tags: Option[String],
                        timely: Option[String],
                        zip_code: Option[String]
                      )

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    try {
      extractData(spark)
    } finally {
      spark.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(sparkAppName)
      .master(sparkMaster)
      .getOrCreate()
  }


  def fetchApiData(frm: Int, size: Int, backend: SttpBackend[Identity, Any]): Either[String, List[Complaint]] = {
    Try(basicRequest
      .get(uri"$apiEndpoint?frm=$frm&size=$size&date_received_min=$startDate&date_received_max=$endDate&product=$productName")
      .send(backend)) match {
      case Success(response) =>
        response.body match {
          case Right(content) =>
            parseData(content)
          case Left(error) =>
            Left(s"API request failed: $error")
        }
      case Failure(exception) =>
        Left(s"Network request failed: ${exception.getMessage}")
    }
  }

  def parseData(content: String): Either[String, List[Complaint]] = {
    Try {
      val data = ujson.read(content)
      val hits = data("hits")("hits").arr
      val complaints = hits.map(hit => parseComplaint(hit("_source").obj))
      Right(complaints.toList)
    } match {
      case Success(result) => result
      case Failure(exception) =>
        Left(s"Failed to parse JSON: ${exception.getMessage}")
    }
  }

  def parseComplaint(json: ujson.Obj): Complaint = Complaint(
    company = json.obj.get("company").flatMap(_.strOpt),
    company_public_response = json.obj.get("company_public_response").flatMap(_.strOpt),
    company_response = json.obj.get("company_response").flatMap(_.strOpt),
    complaint_id = json.obj.get("complaint_id").flatMap(_.strOpt),
    complaint_what_happened = json.obj.get("complaint_what_happened").flatMap(_.strOpt),
    consumer_consent_provided = json.obj.get("consumer_consent_provided").flatMap(_.strOpt),
    consumer_disputed = json.obj.get("consumer_disputed").flatMap(_.strOpt),
    date_received = json.obj.get("date_received").flatMap(_.strOpt),
    date_sent_to_company = json.obj.get("date_sent_to_company").flatMap(_.strOpt),
    has_narrative = json.obj.get("has_narrative").flatMap(_.boolOpt),
    issue = json.obj.get("issue").flatMap(_.strOpt),
    product = json.obj.get("product").flatMap(_.strOpt),
    state = json.obj.get("state").flatMap(_.strOpt),
    sub_issue = json.obj.get("sub_issue").flatMap(_.strOpt),
    sub_product = json.obj.get("sub_product").flatMap(_.strOpt),
    submitted_via = json.obj.get("submitted_via").flatMap(_.strOpt),
    tags = json.obj.get("tags").flatMap(_.strOpt),
    timely = json.obj.get("timely").flatMap(_.strOpt),
    zip_code = json.obj.get("zip_code").flatMap(_.strOpt)
  )

  def extractData(spark: SparkSession): Unit = {
    val backend = HttpURLConnectionBackend()
    var hasMoreData = true
    var totalRecordsExtracted = 0
    var frm = 0

    import spark.implicits._

    while (hasMoreData) {
      fetchApiData(frm, size, backend) match {
        case Right(complaints) =>
          val complaintsDF = complaints.toDF()
          val tempPath = s"/tmp/complaints_data_$frm.parquet"
          // Write DataFrame to Parquet
          complaintsDF.coalesce(1).write.mode("overwrite").parquet(tempPath)

          // Find the Parquet file and upload to S3
          val dir = new java.io.File(tempPath)
          val actualFile = dir.listFiles().find(_.getName.endsWith(".parquet")).get
          uploadFileToS3("credit-card-complaints-raw-data", s"complaints/data_$frm.parquet", actualFile)

          val extractedRecords = complaints.size
          totalRecordsExtracted += extractedRecords
          hasMoreData = extractedRecords == size
          frm += size
        case Left(error) =>
          println(error)
          hasMoreData = false
      }
    }

    println(s"Total records extracted: $totalRecordsExtracted")
  }
}
