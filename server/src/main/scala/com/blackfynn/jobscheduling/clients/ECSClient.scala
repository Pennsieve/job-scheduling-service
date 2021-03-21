// Copyright (c) [2018] - [2021] Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.jobscheduling.clients

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.ecs.AmazonECSAsyncClientBuilder
import com.amazonaws.services.ecs.model._
import com.blackfynn.jobscheduling.clients.AwsAsyncCallback.async
import com.blackfynn.jobscheduling.model.EventualResult.EventualResult

class ECSClient(region: Regions) {

  private val awsECSClient = AmazonECSAsyncClientBuilder
    .standard()
    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
    .withRegion(region)
    .build()

  def runTask(request: RunTaskRequest): EventualResult[RunTaskResult] =
    async(awsECSClient.runTaskAsync)(request)

  def listTasks(request: ListTasksRequest): EventualResult[ListTasksResult] =
    async(awsECSClient.listTasksAsync)(request)

  def stopTask(request: StopTaskRequest): EventualResult[StopTaskResult] =
    async(awsECSClient.stopTaskAsync)(request)

  def describeTasks(request: DescribeTasksRequest): EventualResult[DescribeTasksResult] =
    async(awsECSClient.describeTasksAsync)(request)
}
