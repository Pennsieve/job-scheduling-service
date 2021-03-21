// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.scheduler

import akka.actor.Scheduler
import com.blackfynn.jobscheduling.JobSchedulingPorts.{ createUpdateJob, UpdateJob }
import com.blackfynn.jobscheduling.db.{ Job, JobsMapper }
import com.blackfynn.jobscheduling.pusher.JobPusherPorts.ListTasks
import com.blackfynn.jobscheduling.pusher.{ JobPusher, JobPusherPorts }
import com.blackfynn.jobscheduling.scheduler.JobSchedulerPorts.GetNextJob
import com.blackfynn.jobscheduling.{ JobSchedulingPorts, ServiceConfig }
import com.blackfynn.service.utilities.{ ContextLogger, Tier }

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
