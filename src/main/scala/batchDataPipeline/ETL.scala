package batchDataPipeline

import batchDataPipeline.Complaint
import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._
import sttp.client3._
import scala.util.{Try, Success, Failure}
import AppConfig._
import ujson.{Obj, Value}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter


object ETL {

  // Method to fetch data from the API
  def fetchApiData(startDate: String, endDate: String, frm: Int, pageSize: Int, backend: SttpBackend[Identity, Any]): Either[String, List[Complaint]] = {
    /*
     Attempts to send a GET request to the API with specified parameters.
     Parameters:
       startDate: A string specifying the starting date for the data request.
       endDate: A string specifying the ending date for the data request.
       frm: An integer representing the starting index for pagination.
       pageSize: An integer representing the number of records per page.
       backend: An instance of SttpBackend used to send the request.
     Returns:
       Either[String, List[Complaint]]: On success, returns a list of Complaint objects; on failure, returns an error message.
    */
    Try(basicRequest
      .get(uri"$apiEndpoint?frm=$frm&size=$pageSize&date_received_min=$startDate&date_received_max=$endDate&product=$productName")
      .send(backend)) match {
      case Success(response) =>
        response.body match {
          case Right(content) => parseData(content)
          case Left(error) => Left(s"API request failed: $error")
        }
      case Failure(exception) => Left(s"Network request failed: ${exception.getMessage}")
    }
  }

  // Parse JSON data into Complaint objects
  def parseData(content: String): Either[String, List[Complaint]] = {

    /*
    Parses JSON content and converts it into a list of Complaint objects.
    Parameters:
      content: A String containing the JSON data.
    Returns:
      Either[String, List[Complaint]]: On successful parsing, returns a Right containing a list of Complaint objects.
      On failure, returns a Left with an error message indicating the JSON parsing issue.
    */
    Try {
      val data = ujson.read(content)
      val hits = data("hits")("hits").arr
      val complaints = hits.map(hit => parseComplaint(hit("_source").obj))
      complaints.toList
    } match {
      case Success(result) => Right(result)
      case Failure(exception) => Left(s"Failed to parse JSON: ${exception.getMessage}")
    }
  }

  // Convert JSON objects to Complaint case class instances
  def parseComplaint(json: Obj): Complaint = Complaint(
    /*
    Parses a JSON object into a Complaint object with safely extracted optional fields.
    Parameters:
      json: A ujson.Obj that represents the JSON structure of a complaint record.
    Returns:
      A Complaint object filled with data extracted from the json object.
    Each field in the Complaint object is extracted using flatMap to safely handle cases where the key might not exist or the type conversion is not possible, returning None in such cases.
    Fields are extracted with strOpt for String values and boolOpt for Boolean values to ensure type safety and prevent runtime errors.
    */
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

  // Extract, transform, and load data
  def extractComplaints(startDate: String, endDate: String, backend: SttpBackend[Identity, Any], spark: SparkSession): Either[String, Dataset[Complaint]] = {
    /*
    Extracts complaints data in batches from an API over a given date range, processes it with Spark, and loads it into storage.
    Parameters:
      startDate: A String representing the start date for data extraction.
      endDate: A String representing the end date for data extraction.
      backend: An instance of SttpBackend used for HTTP requests.
      spark: A SparkSession used for data processing.
    Returns:
      Either[String, Dataset[Complaint]]: Returns a Dataset of Complaint objects on success or an error message on failure.
    */
    import spark.implicits._
    val formatter = DateTimeFormatter.ISO_LOCAL_DATE
    var currentStartDate = LocalDate.parse(startDate, formatter)
    val finalEndDate = LocalDate.parse(endDate, formatter)

    var allComplaints = spark.emptyDataset[Complaint]

    while (!currentStartDate.isAfter(finalEndDate)) {
      val currentEndDate = currentStartDate.plusDays(30).minusDays(1)
      val currentStartDateString = currentStartDate.format(formatter)
      val currentEndDateString = currentEndDate.format(formatter)
      val adjustedEndDateString = if (currentEndDate.isAfter(finalEndDate)) endDate else currentEndDateString

      fetchApiData(currentStartDateString, adjustedEndDateString, 0, 10000, backend) match {
        case Right(data) =>
          val currentDataset = spark.createDataset(data) // Assuming data is Seq[Complaint]
          val transformDataset = transformData(currentDataset)
          allComplaints = allComplaints.union(transformDataset)
          loadData(currentDataset, currentStartDate.toEpochDay.toInt) // Load each batch
        case Left(error) =>
          return Left(s"Error fetching data for 30-day period starting $currentStartDateString: $error")
      }

      currentStartDate = currentStartDate.plusDays(30)
    }

    Right(allComplaints)
  }

  // Simple transformation function (as an example)
  def transformData(dataset: Dataset[Complaint]): Dataset[Complaint] = {
    // To be implemented to do more transformation
    dataset.filter(_.complaint_id.isDefined)
  }

  // Load data to Parquet and then upload to S3
  def loadData(dataset: Dataset[Complaint], frm: Int): Unit = {
    /*
    Loads a dataset of Complaints into a temporary parquet file and uploads it to Amazon S3.
    Parameters:
      dataset: A Dataset[Complaint] containing complaint data.
      frm: An integer identifier used to uniquely name the parquet file and organize the uploads.
    Processes:
      1. Data is coalesced into a single partition and written to a parquet file in a temporary directory.
      2. The specific parquet file is identified and uploaded to an S3 bucket.
    Usage:
      This function is typically used to batch process and store large datasets in a distributed cloud storage for further analysis or backup.
    */
    val tempPath = s"/tmp/complaints_data_$frm.parquet"
    dataset.coalesce(1).write.mode("overwrite").parquet(tempPath)
    val dir = new java.io.File(tempPath)
    val actualFile = dir.listFiles().find(_.getName.endsWith(".parquet")).get
    S3Uploader.uploadFileToS3("credit-card-complaints-raw-data", s"complaints/data_$frm.parquet", actualFile)
  }
}
