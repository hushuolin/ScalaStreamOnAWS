import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse}
import software.amazon.awssdk.core.sync.RequestBody
import java.io.File

object S3Uploader {
  val s3: S3Client = S3Client.builder()
    .credentialsProvider(ProfileCredentialsProvider.create("credit-card-complaints"))
    .region(Region.US_EAST_1)
    .build()

  def uploadFileToS3(bucketName: String, key: String, file: File): PutObjectResponse = {
    val putObjectRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()
    val requestBody = RequestBody.fromFile(file.toPath)
    s3.putObject(putObjectRequest, requestBody)
  }
}
