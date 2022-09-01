// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.watchdog
import software.amazon.awssdk.services.ecs.model.Task
import com.pennsieve.jobscheduling.db.TaskId
import com.pennsieve.models.JobId
import io.circe.Encoder
import io.circe.syntax.EncoderOps

case class NoJobIdException(msg: String) extends Exception(msg)

object NoJobIdException {
  def apply(task: Task): NoJobIdException =
    NoJobIdException(s"Could not find valid UUID in environment of task $task")
}

trait WatchDogException extends Exception {
  val msg: String
  val jobId: JobId
  val payloadId: Option[Int]
  val taskId: Option[TaskId]
  val throwable: Throwable

  implicit val encoder: Encoder[WatchDogException] =
    Encoder
      .forProduct4("job_id", "payload_id", "task_id", "throwable")(
        wde => (wde.jobId.toString, wde.payloadId, wde.taskId, wde.throwable.toString)
      )

  override def getMessage: String = s"$msg ${this.asJson.noSpaces}"

  override def getCause: Throwable = throwable
}

case class StoppedTaskWithoutJobException(
  jobId: JobId,
  payloadId: Option[Int],
  taskId: Option[TaskId],
  throwable: Throwable
) extends WatchDogException {
  override val msg: String = "Stopped task without Job"
}

case class FailedToStopTaskException(
  jobId: JobId,
  payloadId: Option[Int],
  taskId: Option[TaskId],
  throwable: Throwable
) extends WatchDogException {
  override val msg: String = "Could not stop task for Job"
}
