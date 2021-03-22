// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.scheduler

import akka.actor.Scheduler
import com.pennsieve.jobscheduling.JobSchedulingPorts.{ createUpdateJob, UpdateJob }
import com.pennsieve.jobscheduling.db.{ Job, JobsMapper }
import com.pennsieve.jobscheduling.pusher.JobPusherPorts.ListTasks
import com.pennsieve.jobscheduling.pusher.{ JobPusher, JobPusherPorts }
import com.pennsieve.jobscheduling.scheduler.JobSchedulerPorts.GetNextJob
import com.pennsieve.jobscheduling.{ JobSchedulingPorts, ServiceConfig }
import com.pennsieve.service.utilities.{ ContextLogger, Tier }

import scala.concurrent.{ ExecutionContext, Future }

case class JobSchedulerPorts(
  getNextJob: GetNextJob,
  listTasks: ListTasks,
  pusher: JobPusher,
  updateJob: UpdateJob
)

object JobSchedulerPorts {
  implicit val jobSchedulerTier: Tier[JobScheduler] = Tier[JobScheduler]

  type GetNextJob = () => Future[Option[Job]]

  def apply(
  )(implicit
    log: ContextLogger,
    ec: ExecutionContext,
    scheduler: Scheduler,
    ports: JobSchedulingPorts,
    config: ServiceConfig
  ): JobSchedulerPorts = {
    val jobPusher =
      new JobPusher(
        ports = JobPusherPorts(ports.ecsClient, ports.manifestClient, ports.db),
        pusherConfig = config.pusher,
        etlBucket = config.s3.etlBucket,
        ecsConfig = config.ecs
      )

    JobSchedulerPorts(
      () => ports.db.run(JobsMapper.getNextJob()),
      ports.ecsClient.listTasks,
      jobPusher,
      createUpdateJob(ports.db)
    )
  }
}
