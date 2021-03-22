// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.scheduler
import akka.actor.Scheduler
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.ActorMaterializer
import com.amazonaws.services.ecs.model.{
  DescribeTasksResult,
  ListTasksResult,
  StopTaskRequest,
  StopTaskResult
}
import com.pennsieve.jobscheduling.JobSchedulingPorts
import com.pennsieve.jobscheduling.JobSchedulingPorts.{ createUpdateJob, UpdateJob }
import com.pennsieve.jobscheduling.TestConfig.{
  staticEcsConfig,
  staticJobSchedulerConfig,
  staticS3Config
}
import com.pennsieve.jobscheduling.db.JobsMapper
import com.pennsieve.jobscheduling.model.ETLEvent
import com.pennsieve.jobscheduling.pusher.JobPusherFakes.{
  defaultEcsConfig,
  defaultPusherConfig,
  successfulRunTask
}
import com.pennsieve.jobscheduling.pusher.JobPusherPorts.{ ListTasks, RunTask }
import com.pennsieve.jobscheduling.pusher.{ JobPusher, JobPusherFakes, JobPusherPorts }
import com.pennsieve.jobscheduling.watchdog.WatchDogPorts.{ DescribeTasks, StopTask }
import com.pennsieve.service.utilities.ContextLogger
import com.pennsieve.test.AwaitableImplicits

import scala.concurrent.{ ExecutionContext, Future }

object JobSchedulerFakes extends AwaitableImplicits {

  val successfulStopTask: StopTask =
    (_: StopTaskRequest) => Future.successful(Right(new StopTaskResult()))

  val emptyDescribeTasks: DescribeTasks = _ => Future.successful(Right(new DescribeTasksResult()))

  val emptyListTasks: ListTasks =
    _ => Future.successful(Right(new ListTasksResult()))

  def jobScheduler(
    runTask: RunTask = successfulRunTask,
    listTasks: ListTasks = emptyListTasks,
    updateJob: Option[UpdateJob] = None
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext,
    matierializer: ActorMaterializer,
    scheduler: Scheduler,
    log: ContextLogger
  ) =
    new JobScheduler(
      staticEcsConfig,
      staticJobSchedulerConfig,
      JobSchedulerPorts(
        () => ports.db.run(JobsMapper.getNextJob()),
        listTasks,
        pusher = new JobPusher(
          JobPusherPorts(
            runTask,
            JobPusherFakes.stubManifestPut,
            JobSchedulingPorts.createGetPayload(ports.db)(_)
          ),
          defaultPusherConfig,
          staticS3Config.etlBucket,
          defaultEcsConfig
        ),
        updateJob.getOrElse(createUpdateJob(ports.db))
      )
    )

  def schedulerAndRunnableEvent(
    runTask: RunTask = successfulRunTask,
    listTasks: ListTasks = emptyListTasks,
    updateJob: Option[UpdateJob] = None
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext,
    matierializer: ActorMaterializer,
    scheduler: Scheduler,
    log: ContextLogger
  ) = {
    val sched = jobScheduler(runTask, listTasks, updateJob)
    val runnableEvent = sched.source.toMat(Sink.headOption)(Keep.right)

    (sched, runnableEvent)
  }

  def runScheduler(
    runTask: RunTask = successfulRunTask,
    listTasks: ListTasks = emptyListTasks,
    updateJob: Option[UpdateJob] = None
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext,
    matierializer: ActorMaterializer,
    scheduler: Scheduler,
    log: ContextLogger
  ): (JobScheduler, Future[Option[ETLEvent]]) = {
    val (jobScheduler, runnableEvent) = schedulerAndRunnableEvent(runTask, listTasks, updateJob)
    (jobScheduler, runnableEvent.run())
  }

  def runFullSchedulerStream(
    runTask: RunTask = successfulRunTask,
    listTasks: ListTasks = emptyListTasks,
    updateJob: Option[UpdateJob] = None
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext,
    matierializer: ActorMaterializer,
    scheduler: Scheduler,
    log: ContextLogger
  ) =
    jobScheduler(runTask, listTasks, updateJob)
      .run(Sink.headOption)
      .awaitFinite()
}
