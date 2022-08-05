// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.scheduler

import akka.NotUsed
import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.QueueOfferResult.{ Dropped, Enqueued, QueueClosed, Failure => QueueFailure }
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Builder
import akka.stream.scaladsl.{ GraphDSL, Keep, Source, SourceQueueWithComplete }
import com.pennsieve.jobscheduling.JobSchedulingPorts.GenericFinalSink
import com.pennsieve.jobscheduling.db.DatabaseClientFlows
import com.pennsieve.jobscheduling.model.ETLEvent
import com.pennsieve.jobscheduling.scheduler.JobArrived.JobArrived
import com.pennsieve.jobscheduling.scheduler.JobNotQueued.JobNotQueued
import com.pennsieve.jobscheduling.scheduler.JobQueued.JobQueued
import com.pennsieve.jobscheduling.shapes.StreamRetry
import com.pennsieve.jobscheduling.{ ECSConfig, JobSchedulerConfig }
import com.pennsieve.service.utilities.ContextLogger

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import JobSchedulerPorts.jobSchedulerTier

class JobScheduler(
  ecsConfig: ECSConfig,
  config: JobSchedulerConfig,
  ports: JobSchedulerPorts
)(implicit
  executionContext: ExecutionContext,
  system: ActorSystem,
  scheduler: Scheduler,
  log: ContextLogger
) {
  type JobQueue = SourceQueueWithComplete[JobArrived]
  type RequestSource = Source[JobArrived, NotUsed]

  private def initializeRequestSource(): (JobQueue, RequestSource) =
    Source
      .queue[JobArrived](config.bufferSize, OverflowStrategy.dropNew)
      .async
      .preMaterialize

  private var (jobQueue, jobRequestSource) = initializeRequestSource()

  def source: Source[ETLEvent, NotUsed] =
    Source
      .fromGraph {
        GraphDSL.create() { implicit builder: Builder[NotUsed] =>
          import GraphDSL.Implicits._

          val jobSource = builder add new JobSource(ports.getNextJob)
          val sourcesConsumer = builder add new JobSourceConsumer(config.maxTasks, ecsConfig, ports)
          val pusher = builder add ports.pusher.flow

          jobSource ~> sourcesConsumer.in0
          jobRequestSource ~> sourcesConsumer.in1

          sourcesConsumer.out ~> pusher.in
          SourceShape(pusher.out)
        }
      }

  def closeQueue: Unit = jobQueue.fail(StreamShutdownException)

  private def restartQueueAndRun[A](finalSink: GenericFinalSink[A]): () => Future[A] = () => {
    if (jobQueue.watchCompletion.isCompleted) {
      val restarted = initializeRequestSource()
      jobQueue = restarted._1
      jobRequestSource = restarted._2
    }
    source
      .via(DatabaseClientFlows.updateDatabaseFlow(config.throttle, ports.updateJob, () => addJob))
      .toMat(finalSink)(Keep.right)
      .run()
  }

  def run[A](finalSink: GenericFinalSink[A]): Future[A] =
    StreamRetry(restartQueueAndRun(finalSink), config.retry, "JobScheduler")

  def addJob(): Future[Either[JobNotQueued, JobQueued]] =
    jobQueue
      .offer(JobArrived)
      .map {
        case QueueClosed => Left(JobNotQueued)

        case QueueFailure(_) => Left(JobNotQueued)

        case Dropped => Right(JobQueued)

        case Enqueued => Right(JobQueued)
      }
      .recover {
        case NonFatal(_) => Left(JobNotQueued)
      }
}

case object JobArrived {
  type JobArrived = this.type
}

case object JobQueued {
  type JobQueued = this.type
}

case object JobNotQueued {
  type JobNotQueued = this.type
}

case object StreamShutdownException extends Exception
