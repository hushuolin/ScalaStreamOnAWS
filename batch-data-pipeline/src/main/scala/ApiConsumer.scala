import sttp.client3._
import ujson._
import org.apache.spark.sql.SparkSession


object ApiConsumer {
  val apiEndpoint = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
  val backend = HttpURLConnectionBackend()
  var frm = 0
  val size = 1000 // Adjust size based on API and network performance
  val start_date = "2023-09-01" // Example start date
  val end_date = "2023-09-02"  // Example end date
  val spark = SparkSession.builder()
    .appName("API Consumer with Spark")
    .master("local[*]") // Use local in a non-clustered environment
    .getOrCreate()
  import spark.implicits._

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
    try {
      extractData()
    } finally {
      spark.stop()
    }
  }

  def extractData(): Unit = {
    var hasMoreData = true
    var totalRecordsExtracted = 0

    while (hasMoreData) {
      val response = basicRequest
        .get(uri"$apiEndpoint?frm=$frm&size=$size&date_received_min=$start_date&date_received_max=$end_date&product=Credit%20card")
        .send(backend)

      response.body match {
        case Right(content) =>
          val data = ujson.read(content)
          // Assuming `data` contains a JSON object with an array under "hits" -> "hits"
          val hits = data("hits")("hits")

          // Print the first few elements of the array to preview their contents
          hits.arr.take(5).foreach(println)


          val complaintsJson = data("hits")("hits").arr.map(_.obj("_source"))

          val complaints = complaintsJson.map { json =>
            Complaint(
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
          }.toList


          val complaintsDF = complaints.toDF()

          val complaintIdDF = complaintsDF.select("complaint_id")

          // Show the first 10 rows of the complaint_id column
          complaintsDF.show(10, truncate = false)

          val extractedRecords = complaints.size
          totalRecordsExtracted += extractedRecords
          if (extractedRecords < size) {
            hasMoreData = false
          } else {
            frm += size
          }

        case Left(error) =>
          println(s"API request failed: $error")
          hasMoreData = false
      }
    }

    println(s"Total records extracted: $totalRecordsExtracted")
  }

}
