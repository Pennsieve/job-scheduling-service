// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import software.amazon.awssdk.services.ecs.model.Task
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

final case class TaskId(taskArn: String, clusterArn: String)

object TaskId {

  def fromTask(task: Task): Option[TaskId] =
    if (task.taskArn == null || task.clusterArn == null) {
      None
    } else Some(TaskId(task.taskArn, task.clusterArn))

  implicit def encoder: Encoder[TaskId] = deriveEncoder[TaskId]
  implicit def decoder: Decoder[TaskId] = deriveDecoder[TaskId]
}
