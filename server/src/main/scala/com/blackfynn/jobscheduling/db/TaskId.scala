// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.db

import com.amazonaws.services.ecs.model.Task
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

final case class TaskId(taskArn: String, clusterArn: String)

object TaskId {

  def fromTask(task: Task): Option[TaskId] =
    if (task.getTaskArn == null || task.getClusterArn == null) {
      None
    } else Some(TaskId(task.getTaskArn, task.getClusterArn))

  implicit def encoder: Encoder[TaskId] = deriveEncoder[TaskId]
  implicit def decoder: Decoder[TaskId] = deriveDecoder[TaskId]
}
