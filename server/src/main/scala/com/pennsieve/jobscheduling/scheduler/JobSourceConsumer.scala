// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.scheduler

import akka.actor.Scheduler
import akka.pattern.after
import akka.stream._
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import cats.Monad
import cats.implicits._
import software.amazon.awssdk.services.ecs.model.{ ListTasksRequest, ListTasksResponse }
import com.pennsieve.jobscheduling.ECSConfig
import com.pennsieve.jobscheduling.db.Job
import com.pennsieve.jobscheduling.scheduler.JobArrived.JobArrived
import com.pennsieve.service.utilities.ContextLogger
import io.circe.syntax.EncoderOps

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }
import JobSchedulerPorts.jobSchedulerTier

class JobSourceConsumer(
  maxTasks: Int,
  ecsConfig: ECSConfig,
  ports: JobSchedulerPorts
)(implicit
  log: ContextLogger,
  scheduler: Scheduler,
  ec: ExecutionContext
) extends GraphStage[FanInShape2[Option[Job], JobArrived, Job]] {
  override val shape =
    new FanInShape2[Option[Job], JobArrived, Job]("SourceConsumer")

  val nextJobIn: Inlet[Option[Job]] = shape.in0

  val attemptIn: Inlet[JobArrived] = shape.in1

  val nextJobOut: Outlet[Job] = shape.out

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val listTasksCallback = getAsyncCallback[Unit](_ => tryPull(nextJobIn))

      private def tryGetNextJob: Unit =
        recurseTilCapacityAvailable onComplete {
          case Success(_) => listTasksCallback.invoke(())
          case Failure(exception) =>
            log.tierNoContext
              .error("cannot list tasks this message should never be seen", exception)
        }

      setHandler(
        nextJobIn,
        new InHandler {
          override def onPush(): Unit =
            grab(nextJobIn) match {
              case Some(job) =>
                log.tierContext
                  .info(s"got next job ${job.asJson.noSpaces}")(job.logContext)
                push(nextJobOut, job)

              case None =>
                log.tierNoContext.warn("attempted to get job found nothing")
                if (!hasBeenPulled(attemptIn)) tryPull(attemptIn)
            }
        }
      )

      setHandler(attemptIn, new InHandler {
        override def onPush(): Unit = {
          log.tierNoContext.info("was notified of new job")
          grab(attemptIn)
          if (isAvailable(nextJobOut)) tryGetNextJob
        }
      })

      setHandler(nextJobOut, new OutHandler {
        override def onPull(): Unit = {
          log.tierNoContext.info("JobSourcesConsumer was pulled by JobPusher")
          tryGetNextJob
        }
      })
    }

  private val listTasksRequest =
    ListTasksRequest
      .builder()
      .cluster(ecsConfig.cluster)
      .desiredStatus("RUNNING")
      .launchType("FARGATE")
      .build()

  private def safeListTasksAsync: Future[ListTasksResponse] =
    ports
      .listTasks(listTasksRequest)
      .flatMap {
        case Right(listTask) =>
          log.tierNoContext.info(s"Successfully listed tasks for: ${listTask.taskArns.size} tasks")
          Future.successful(listTask)

        case Left(left) =>
          log.tierNoContext.warn(s"Failed to list tasks for: ${left.toString}")

          left.tailRecM[Future, ListTasksResponse] { _ =>
            after(1.second, scheduler) {
              ports.listTasks(listTasksRequest)
            }
          }
      }

  // Tail recursive with cats magic?
  // http://eed3si9n.com/herding-cats/tail-recursive-monads.html
  private def recurseTilCapacityAvailable: Future[Unit] =
    safeListTasksAsync.flatMap { initialListTasksResult =>
      Monad[Future].tailRecM(initialListTasksResult) { listTasksResult =>
        if (listTasksResult.taskArns.size < maxTasks) {
          log.tierNoContext.info(
            s"found spare capacity of ${maxTasks - listTasksResult.taskArns.size} in ecs"
          )
          Future.successful(Right(()))
        } else
          after(1.second, scheduler) {
            log.tierNoContext.info(s"checking for spare capacity in ecs")
            safeListTasksAsync.map { listTasksResult =>
              Left(listTasksResult)
            }
          }
      }
    }
}
