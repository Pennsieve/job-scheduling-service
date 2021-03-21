// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.errors

import akka.http.scaladsl.model.HttpResponse
import com.blackfynn.core.clients.packages.UploadCompleteResponse
import com.blackfynn.jobscheduling.commons.JobState
import com.blackfynn.jobscheduling.db.Job
import com.blackfynn.models.{ JobId, Payload }

import scala.reflect.ClassTag

case object ForbiddenException extends Throwable
case class InvalidJobIdException(jobId: String) extends Throwable
case class DatabaseException(cause: Throwable) extends Throwable

case class NoJobException(jobId: JobId) extends Throwable {
  override def getMessage: String = s"No job could be found for jobId=$jobId"
}

case class NoTaskException(jobId: JobId) extends Throwable {
  override def getMessage: String = s"No task could be found for jobId=$jobId"
}

case class TaskMissingArnException(jobId: JobId) extends Exception {
  override def getMessage: String =
    s"Task associated with jobId=$jobId had a null cluster ARN or task ARN."
}

case class NoTaskIdException(msg: String = "") extends Exception

case class NoPayloadException(payloadId: Int) extends Exception {
  override def getMessage: String =
    s"No payload could be found for payloadId=$payloadId"
}

case object NoPayloadForJob extends Exception

case class UnsupportedPayload(payload: Payload)
    extends Exception(s"Does not currently support payload type $payload")

case class UnsupportedUploadPayload(job: Job, payload: Payload)
    extends Exception(
      s"Job: ${job.id} not associated with an Upload payload. Actually: ${payload.getClass.getCanonicalName}"
    )

case class InvalidJobState(job: Job)
    extends Exception(
      s"Job: ${job.id} is in an invalid state ${job.state} for the current operation."
    )

case class InvalidStateUpdate(job: Job, newState: JobState)
    extends Exception(s"Job: ${job.id} could not be updated with state $newState")

case object SameStateTransitionIgnoring
    extends Exception("Trying to update a state to it's current value. Ignoring.")

case object TerminalStateTransitionIgnoring
    extends Exception("Trying to update a state already in a terminal state. Ignoring.")

case class PackagesUploadCompleteError(uploadCompleteResponse: UploadCompleteResponse)
    extends Exception(s"Error calling packages upload-complete: $uploadCompleteResponse")

case class HttpClientUnsupportedResponse[T](
  httpResponse: HttpResponse
)(implicit
  clientClassTag: ClassTag[T]
) extends Exception(
      s"Http Client ${clientClassTag.runtimeClass.getCanonicalName} unsupported response: $httpResponse"
    )
