package batchDataPipeline

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
