// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.scheduler

import akka.actor.Scheduler
import akka.pattern.after
import akka.stream._
import akka.stream.stage.{ GraphStage, GraphStageLogic, OutHandler }
import com.blackfynn.jobscheduling.db.Job
import com.blackfynn.jobscheduling.scheduler.JobSchedulerPorts.GetNextJob
import com.blackfynn.service.utilities.ContextLogger
import io.circe.syntax.EncoderOps

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future }
import scala.math.random
import scala.util.{ Failure, Success }
import JobSchedulerPorts.jobSchedulerTier

class JobSource(
  getNextJob: GetNextJob
)(implicit
  ec: ExecutionContext,
  log: ContextLogger,
  scheduler: Scheduler
) extends GraphStage[SourceShape[Option[Job]]] {

  val jobSourceOut: Outlet[Option[Job]] =
    Outlet[Option[Job]]("JobSourceOutlet")

  override val shape = SourceShape[Option[Job]](jobSourceOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val callback = getAsyncCallback[Option[Job]](push(jobSourceOut, _))

      // PREVENTS REPEATED DEADLOCKS BETWEEN MULTIPLE INSTANCES
      private def tenSecondsPlusRandom = (10 + (random() * 10).toInt) seconds

      def retryingGetNextJob(): Future[Option[Job]] =
        getNextJob()
          .recoverWith {
            case e =>
              log.tierNoContext.error(s"JobSource error during getNextJob: ${e.getMessage}")
              after(tenSecondsPlusRandom, scheduler)(getNextJob())
          }

      setHandler(
        jobSourceOut,
        new OutHandler {
          override def onPull(): Unit =
            retryingGetNextJob() onComplete {
              case Success(job) =>
                log.noContext
                  .info(s"JobSource tried to get job got ${job.fold("NONE")(_.asJson.noSpaces)}")
                callback.invoke(job)

              case Failure(e) =>
                // THIS SHOULD NEVER OCCUR
                log.tierNoContext.error(
                  s"JobSource error during getNextJob failing stage: ${e.getMessage}"
                )
                // TEARS DOWN STREAM WHICH SHOULD BE RESTARTED
                failStage(e)
            }
        }
      )
    }
}
