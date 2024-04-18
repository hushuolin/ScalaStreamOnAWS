import batchDataPipeline.Complaint
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import sttp.client3._
import batchDataPipeline.ETL.fetchApiData
import org.mockito.ArgumentMatchers.any
import sttp.model.StatusCode

class FetchApiDataTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "fetchApiData" should "return a list of Complaints on successful API response" in {
    val mockBackend = mock[SttpBackend[Identity, Any]]
    val apiResponse = Response.ok[Either[String, String]](Right("""
      {"hits": {"hits": [{"_source": {
        "complaint_id": "123",
        "issue": "Example issue",
        "company": "XYZ Corp",
        "company_public_response": "Thank you for your feedback",
        "company_response": "Closed with explanation",
        "complaint_what_happened": "Issue with credit card charges",
        "consumer_consent_provided": "Consent provided",
        "consumer_disputed": "No",
        "date_received": "2021-01-01",
        "date_sent_to_company": "2021-01-02",
        "has_narrative": true,
        "product": "Credit card",
        "state": "CA",
        "sub_issue": "Unauthorized charge",
        "sub_product": "General credit card",
        "submitted_via": "Email",
        "tags": "Elder",
        "timely": "Yes",
        "zip_code": "90210"
      }}]}}
    """))
    when(mockBackend.send(any[Request[Either[String, String], Any]]))
      .thenReturn(apiResponse)

    val expectedComplaints = List(Complaint(
      Some("XYZ Corp"), Some("Thank you for your feedback"), Some("Closed with explanation"),
      Some("123"), Some("Issue with credit card charges"), Some("Consent provided"),
      Some("No"), Some("2021-01-01"), Some("2021-01-02"), Some(true),
      Some("Example issue"), Some("Credit card"), Some("CA"),
      Some("Unauthorized charge"), Some("General credit card"), Some("Email"),
      Some("Elder"), Some("Yes"), Some("90210")
    ))

    val result = fetchApiData("2021-01-01", "2021-01-31", 0, 100, mockBackend)
    result shouldBe Right(expectedComplaints)
  }

  it should "return an error message on API request failure" in {
    val mockBackend = mock[SttpBackend[Identity, Any]]
    val apiResponse = Response.apply[Either[String, String]](Left("Not found"), StatusCode.NotFound, "Not Found")
    when(mockBackend.send(any[Request[Either[String, String], Any]]))
      .thenReturn(apiResponse)

    val result = fetchApiData("2021-01-01", "2021-01-31", 0, 100, mockBackend)
    result should be(Left("API request failed: Not found"))
  }

  it should "return an error message on network failure" in {
    val mockBackend = mock[SttpBackend[Identity, Any]]
    when(mockBackend.send(any[Request[Either[String, String], Any]]))
      .thenThrow(new RuntimeException("Network error"))

    val result = fetchApiData("2021-01-01", "2021-01-31", 0, 100, mockBackend)
    result shouldBe Left("Network request failed: Network error")
  }
}
