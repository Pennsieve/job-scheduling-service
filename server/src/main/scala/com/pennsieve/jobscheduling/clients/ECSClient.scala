// Copyright (c) [2018] - [2022] Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.jobscheduling.clients

import software.amazon.awssdk.services.ecs.EcsAsyncClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.ecs.model._
import com.pennsieve.jobscheduling.clients.AwsAsyncResponseAdapter.toEventualResult
import com.pennsieve.jobscheduling.model.EventualResult.EventualResult
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

class ECSClient(region: Region) {

  private val awsECSClient = EcsAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  def runTask(request: RunTaskRequest): EventualResult[RunTaskResponse] =
    toEventualResult(awsECSClient.runTask(request))

  def listTasks(request: ListTasksRequest): EventualResult[ListTasksResponse] =
    toEventualResult(awsECSClient.listTasks(request))

  def stopTask(request: StopTaskRequest): EventualResult[StopTaskResponse] =
    toEventualResult(awsECSClient.stopTask(request))

  def describeTasks(request: DescribeTasksRequest): EventualResult[DescribeTasksResponse] =
    toEventualResult(awsECSClient.describeTasks(request))
}
