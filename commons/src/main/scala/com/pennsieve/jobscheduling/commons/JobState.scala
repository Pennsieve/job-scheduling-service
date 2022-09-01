// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.commons

import enumeratum.{CirceEnum, Enum, EnumEntry}
import enumeratum.EnumEntry.UpperSnakecase

import scala.collection.immutable

sealed abstract class JobState extends EnumEntry with UpperSnakecase

object JobState extends Enum[JobState] with CirceEnum[JobState] {

  val values: immutable.IndexedSeq[JobState] = findValues

  sealed trait NoTaskId

  sealed trait Retryable

  // UPLOAD STATES
  /**
    * PAYLOAD BEING SCANNED AND MOVED TO STORAGE BUCKET
    * This is the starting state for upload and legacy payloads
    */
  case object Uploading extends JobState with Retryable with NoTaskId

  /**
    * MARKED AS READY TO BE SCHEDULED
    * This is the starting state for workflow payloads
    */
  case object Available extends JobState with NoTaskId

  // BEING PUSHED TO ECS
  case object Submitted extends JobState with NoTaskId

  // ARRIVED AT ECS
  case object Sent extends JobState

  case object Pending extends JobState

  case object Running extends JobState

  /**
    * PAYLOAD DID NOT NEED TO BE PROCESSED
    * Used to signify that an upload job is in a completed state
    */
  case object NotProcessing extends JobState with NoTaskId

  // COMPLETED IN ECS
  case object Succeeded extends JobState with NoTaskId
  // JOB WAS CANCELLED
  case object Cancelled extends JobState with NoTaskId

  // FAILED IN ECS
  case object Failed extends JobState with NoTaskId

  // ECS TASK CANNOT BE FOUND FOR JOB
  case object Lost extends JobState with NoTaskId

  val inMotionStates: Set[JobState] = Set(Submitted, Sent, Pending, Running)

  val terminalStates: Set[JobState] = Set(NotProcessing, Succeeded, Failed, Lost)
}
