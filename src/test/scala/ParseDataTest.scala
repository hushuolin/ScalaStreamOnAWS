import batchDataPipeline.Complaint
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import batchDataPipeline.ETL.parseData

class ParseDataTest extends AnyFlatSpec with Matchers {

  "parseData" should "correctly parse valid JSON data into a list of Complaint objects" in {
    val jsonInput = """
      {
        "hits": {
          "hits": [
            {
              "_source": {
                "company": "XYZ Corp",
                "company_public_response": "Thank you for your feedback",
                "company_response": "Closed with explanation",
                "complaint_id": "123456",
                "complaint_what_happened": "Issue with credit card charges",
                "consumer_consent_provided": "Consent provided",
                "consumer_disputed": "No",
                "date_received": "2021-01-01",
                "date_sent_to_company": "2021-01-02",
                "has_narrative": true,
                "issue": "Incorrect charges",
                "product": "Credit card",
                "state": "CA",
                "sub_issue": "Unauthorized charge",
                "sub_product": "General credit card",
                "submitted_via": "Email",
                "tags": "Elder",
                "timely": "Yes",
                "zip_code": "90210"
              }
            }
          ]
        }
      }
    """

    val expectedComplaints = List(
      Complaint(
        Some("XYZ Corp"), Some("Thank you for your feedback"), Some("Closed with explanation"),
        Some("123456"), Some("Issue with credit card charges"), Some("Consent provided"),
        Some("No"), Some("2021-01-01"), Some("2021-01-02"), Some(true),
        Some("Incorrect charges"), Some("Credit card"), Some("CA"),
        Some("Unauthorized charge"), Some("General credit card"), Some("Email"),
        Some("Elder"), Some("Yes"), Some("90210")
      )
    )

    val result = parseData(jsonInput)
    result should be(Right(expectedComplaints))
  }

  it should "return an error if the JSON is invalid" in {
    val invalidJsonInput = "{ this is not : valid JSON }"
    val result = parseData(invalidJsonInput)
    result.isLeft shouldBe true
  }
}
