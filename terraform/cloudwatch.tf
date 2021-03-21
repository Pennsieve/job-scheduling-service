// ALARM TO NOTIFY THAT MAIN SCHEDULING STREAM HAS CRASHED WITH EXCEPTION

resource "aws_cloudwatch_log_metric_filter" "job_scheduler_stream_failed_to_exit_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-job-scheduler-failed-to-exit-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-job-scheduler-failed-to-exit-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "Scheduler completed without processing all events"
}

resource "aws_cloudwatch_metric_alarm" "job_scheduler_stream_failed_to_exit" {
  alarm_name          = "${var.environment_name}-${var.service_name}-job-scheduler-stream-failed-to-exit-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.job_scheduler_stream_failed_to_exit_filter.name
  namespace           = var.alarm_namespace

  # 1 minute
  period             = 60
  statistic          = "Sum"
  threshold          = 1
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM NOTIFY THAT WATCH DOG STREAM HAS CRASHED WITH EXCEPTION

resource "aws_cloudwatch_log_metric_filter" "watch_dog_stream_failed_to_exit_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-watch-dog-failed-to-exit-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-watch-dog-failed-to-exit-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "WatchDog completed without processing all events"
}

resource "aws_cloudwatch_metric_alarm" "watch_dog_stream_failed_to_exit" {
  alarm_name          = "${var.environment_name}-${var.service_name}-watch-dog-stream-failed-to-exit-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.watch_dog_stream_failed_to_exit_filter.name
  namespace           = var.alarm_namespace

  # 1 minute
  period             = 60
  statistic          = "Sum"
  threshold          = 1
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM TO NOTIFY THAT THERE HAVE BEEN MORE THAN 10 UNPROCESSED EVENTS IN A 5 MINUTE PERIOD FOR SCHEDULER STREAM

resource "aws_cloudwatch_log_metric_filter" "scheduler_event_sink_high_event_failures_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-scheduler-event-sink-high-event-failure-rate-filter-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-scheduler-event-sink-high-event-failure-rate-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "Scheduler failed to complete event"
}

resource "aws_cloudwatch_metric_alarm" "scheduler_event_sink_high_event_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-scheduler-event-sink-high-event-failure-rate-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.scheduler_event_sink_high_event_failures_filter.name
  namespace           = var.alarm_namespace

  # 5 minutes
  period             = 300
  statistic          = "Sum"
  threshold          = 10
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM TO NOTIFY THAT THERE HAVE BEEN MORE THAN 10 UNPROCESSED EVENTS IN A 5 MINUTE PERIOD FOR WATCH DOG

resource "aws_cloudwatch_log_metric_filter" "watch_dog_event_sink_high_event_failures_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-watch-dog-event-sink-high-event-failure-rate-filter-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-watch-dog-event-sink-high-event-failure-rate-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "WatchDog failed to complete event"
}

resource "aws_cloudwatch_metric_alarm" "watch_dog_event_sink_high_event_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-watch-dog-event-sink-high-event-failure-rate-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.watch_dog_event_sink_high_event_failures_filter.name
  namespace           = var.alarm_namespace

  # 5 minutes
  period             = 300
  statistic          = "Sum"
  threshold          = 10
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM TO NOTIFY THAT THERE HAVE BEEN MORE THAN 10 FAILED REQUESTS TO UPLOAD COMPLETE IN 1 MINUTE

resource "aws_cloudwatch_log_metric_filter" "upload_complete_response_failures_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-upload-complete-filter-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-upload-complete-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "UploadComplete failed to complete upload"
}

resource "aws_cloudwatch_metric_alarm" "upload_complete_response_high_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-upload-complete-high-failures-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.upload_complete_response_failures_filter.name
  namespace           = var.alarm_namespace

  # 5 minutes
  period             = 60
  statistic          = "Sum"
  threshold          = 10
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM TO NOTIFY THAT THERE HAVE BEEN MORE THAN 10 FAILED REQUESTS TO CREATE JOB IN 1 MINUTE

resource "aws_cloudwatch_log_metric_filter" "create_job_response_failures_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-create-job-filter-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-create-job-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "CreateJob failed to create job"
}

resource "aws_cloudwatch_metric_alarm" "create_job_response_high_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-create-job-high-failures-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.create_job_response_failures_filter.name
  namespace           = var.alarm_namespace

  # 5 minutes
  period             = 60
  statistic          = "Sum"
  threshold          = 10
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM TO NOTIFY THAT THERE HAVE BEEN MORE THAN 10 FAILED REQUESTS TO GET JOB IN 1 MINUTE

resource "aws_cloudwatch_log_metric_filter" "get_job_response_failures_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-get-job-filter-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-get-job-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "GetJob failed to get job"
}

resource "aws_cloudwatch_metric_alarm" "get_job_response_high_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-get-job-high-failures-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.get_job_response_failures_filter.name
  namespace           = var.alarm_namespace

  # 5 minutes
  period             = 60
  statistic          = "Sum"
  threshold          = 10
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}

// ALARM TO NOTIFY THAT THERE HAVE BEEN MORE THAN 10 FAILED REQUESTS TO GET ALL JOBS IN 1 MINUTE

resource "aws_cloudwatch_log_metric_filter" "get_all_jobs_response_failures_filter" {
  log_group_name = data.terraform_remote_state.ecs_cluster.outputs.cloudwatch_log_group_name

  metric_transformation {
    name          = "${var.environment_name}-${var.service_name}-get-all-jobs-filter-transformation-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
    namespace     = var.alarm_namespace
    value         = "1"
    default_value = "0"
  }

  name    = "${var.environment_name}-${var.service_name}-get-all-jobs-filter-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  pattern = "GetAllJobs failed to get all jobs"
}

resource "aws_cloudwatch_metric_alarm" "get_all_jobs_response_high_failures" {
  alarm_name          = "${var.environment_name}-${var.service_name}-get-all-jobs-high-failures-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.get_all_jobs_response_failures_filter.name
  namespace           = var.alarm_namespace

  # 5 minutes
  period             = 60
  statistic          = "Sum"
  threshold          = 10
  actions_enabled    = false
  alarm_actions      = [data.terraform_remote_state.account.outputs.data_management_victor_ops_sns_topic_arn]
  treat_missing_data = "ignore"
}
