// Copyright (c) [2018] - [2025] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.scheduler
import akka.actor.{ ActorSystem, Scheduler }
import akka.stream.scaladsl.{ Keep, Sink }
import software.amazon.awssdk.services.ecs.model.{
  DescribeTasksResponse,
  ListTasksResponse,
  StopTaskRequest,
  StopTaskResponse
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
    (_: StopTaskRequest) => Future.successful(Right(StopTaskResponse.builder().build()))

  val emptyDescribeTasks: DescribeTasks = _ =>
    Future.successful(Right(DescribeTasksResponse.builder().build()))

  val emptyListTasks: ListTasks =
    _ => Future.successful(Right(ListTasksResponse.builder().build()))

  def jobScheduler(
    runTask: RunTask = successfulRunTask,
    listTasks: ListTasks = emptyListTasks,
    updateJob: Option[UpdateJob] = None
  )(implicit
    ports: JobSchedulingPorts,
    ec: ExecutionContext,
    system: ActorSystem,
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
    system: ActorSystem,
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
    system: ActorSystem,
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
    system: ActorSystem,
    scheduler: Scheduler,
    log: ContextLogger
  ) =
    jobScheduler(runTask, listTasks, updateJob)
      .run(Sink.headOption)
      .awaitFinite()
}
