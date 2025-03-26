// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import software.amazon.awssdk.auth.signer.Aws4Signer
import software.amazon.awssdk.services.s3.S3Client
import com.pennsieve.jobscheduling.thirdparty.aws.AmazonS3URI
import com.pennsieve.jobscheduling.S3Config
import com.pennsieve.jobscheduling.model.ManifestUri
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import com.pennsieve.jobscheduling.pusher.ManifestUploadFailure
import com.pennsieve.models.Manifest
import io.circe.parser.decode
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.client.config.{
  ClientOverrideConfiguration,
  SdkAdvancedClientOption
}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.{ GetObjectRequest, PutObjectRequest }

import scala.concurrent.{ blocking, ExecutionContext, Future }
import scala.io.Source
import scala.util.{ Failure, Success, Try }

class ManifestS3Client(awsS3Client: S3Client, etlBucket: String) {

  def putManifest(
    manifestS3: ManifestS3,
    executionContext: ExecutionContext
  ): EventualResult[Unit] =
    Future {

      /**
        * The below code is blocking allowing it to run on the main thread pool
        * will cause threads in the main thread pool to block. Best practice is
        * to wrap blocking code in a blocking context. Which will force it to be
        * spawned on a fresh thread outside the pool. Stopping the pool from becoming deadlocked.
        *
        *
        * https://docs.scala-lang.org/overviews/core/futures.html#blocking
        */
      blocking[Either[ManifestUploadFailure, Unit]] {
        val attempt = Try {

          val putObjectRequest: PutObjectRequest = PutObjectRequest
            .builder()
            .contentType("application/json")
            .contentLength(manifestS3.bytes.length)
            .bucket(etlBucket)
            .key(manifestS3.uri)
            .build()

          awsS3Client
            .putObject(putObjectRequest, RequestBody.fromBytes(manifestS3.bytes))
        }

        attempt match {
          case Success(_) => Right(())
          case Failure(exception) => Left(ManifestUploadFailure(exception))
        }
      }
    }(executionContext)

  def getManifest(uri: ManifestUri)(implicit ec: ExecutionContext): EventualResult[Manifest] =
    Future {

      /**
        * The below code is blocking allowing it to run on the main thread pool
        * will cause threads in the main thread pool to block. Best practice is
        * to wrap blocking code in a blocking context. Which will force it to be
        * spawned on a fresh thread outside the pool. Stopping the pool from becoming deadlocked.
        *
        * https://docs.scala-lang.org/overviews/core/futures.html#blocking
        */
      blocking {
        for {
          uri <- Try(new AmazonS3URI(uri.toString)).toEither
          bucket = uri.getBucket
          key = uri.getKey
          getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()
          s3Object <- Try(awsS3Client.getObjectAsBytes(getObjectRequest)).toEither
          manifestJson = Source
            .fromBytes(s3Object.asByteArray())
            .getLines()
            .mkString("\n")
          manifest <- decode[Manifest](manifestJson)
        } yield manifest
      }
    }

}

object ManifestS3Client {

  def apply(s3Config: S3Config): ManifestS3Client = {
    val amazonS3Client =
      S3Client
        .builder()
        .credentialsProvider(DefaultCredentialsProvider.create())
        .region(s3Config.awsRegion)
        .build()

    new ManifestS3Client(amazonS3Client, s3Config.etlBucket)
  }
}

case class ManifestS3(uri: String, bytes: Array[Byte])
