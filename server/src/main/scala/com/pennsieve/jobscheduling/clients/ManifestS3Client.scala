// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import java.io.ByteArrayInputStream

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder, AmazonS3URI }
import com.pennsieve.jobscheduling.S3Config
import com.pennsieve.jobscheduling.model.ManifestUri
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import com.pennsieve.jobscheduling.pusher.ManifestUploadFailure
import com.pennsieve.models.Manifest
import io.circe.parser.decode

import scala.concurrent.{ blocking, ExecutionContext, Future }
import scala.io.Source
import scala.util.{ Failure, Success, Try }

class ManifestS3Client(awsS3Client: AmazonS3, etlBucket: String) {

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
          val metadata = new ObjectMetadata()

          metadata.setContentType("application/json")
          metadata.setContentLength(manifestS3.bytes.length)

          awsS3Client
            .putObject(
              etlBucket,
              manifestS3.uri,
              new ByteArrayInputStream(manifestS3.bytes),
              metadata
            )
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
          s3Object <- Try(awsS3Client.getObject(bucket, key)).toEither
          manifestJson = Source
            .fromInputStream(s3Object.getObjectContent)
            .getLines()
            .mkString("\n")
          _ <- Try(s3Object.close()).toEither
          manifest <- decode[Manifest](manifestJson)
        } yield manifest
      }
    }

}

object ManifestS3Client {

  val clientConfiguration: ClientConfiguration =
    new ClientConfiguration()
      .withSignerOverride("AWSS3V4SignerType")

  def apply(s3Config: S3Config): ManifestS3Client = {
    val amazonS3Client =
      AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(clientConfiguration)
        .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
        .withRegion(s3Config.awsRegion)
        .build()

    new ManifestS3Client(amazonS3Client, s3Config.etlBucket)
  }
}

case class ManifestS3(uri: String, bytes: Array[Byte])
