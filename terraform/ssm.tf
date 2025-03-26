# AWS Region

resource "aws_ssm_parameter" "region" {
  name  = "/${var.environment_name}/${var.service_name}/region"
  type  = "String"
  value = data.aws_region.current_region.name
}

# Cloudwatch Configuration

resource "aws_ssm_parameter" "nextflow_status_queue_id" {
  name  = "/${var.environment_name}/${var.service_name}/nextflow-status-queue-id"
  type  = "String"
  value = data.terraform_remote_state.etl_infrastructure.outputs.nextflow_status_queue_id
}

resource "aws_ssm_parameter" "cloudwatch_parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/cloudwatch-parallelism"
  type  = "String"
  value = var.cloudwatch_parallelism
}

resource "aws_ssm_parameter" "monitor_max_retries" {
  name  = "/${var.environment_name}/${var.service_name}/monitor-max-retries"
  type  = "String"
  value = var.monitor_max_retries
}

resource "aws_ssm_parameter" "monitor_retry_delay" {
  name  = "/${var.environment_name}/${var.service_name}/monitor-retry-delay"
  type  = "String"
  value = var.monitor_retry_delay
}

resource "aws_ssm_parameter" "monitor_retry_reset_after" {
  name  = "/${var.environment_name}/${var.service_name}/monitor-retry-reset-after"
  type  = "String"
  value = var.monitor_retry_reset_after
}

# ECS Configuration

resource "aws_ssm_parameter" "ecs_cluster_arn" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-cluster-arn"
  type  = "String"
  value = data.terraform_remote_state.fargate.outputs.ecs_cluster_arn
}

resource "aws_ssm_parameter" "ecs_security_groups" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-security-groups"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.ecs_ec2_security_group_id
}

resource "aws_ssm_parameter" "ecs_subnet_ids" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-subnet-ids"
  type  = "String"
  value = join(",", data.terraform_remote_state.vpc.outputs.private_subnet_ids)
}

resource "aws_ssm_parameter" "ecs_iam_access_key_id" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-iam-access-key-id"
  type  = "String"
  value = data.terraform_remote_state.etl_infrastructure.outputs.nextflow_iam_access_key_id
}

resource "aws_ssm_parameter" "ecs_iam_access_key_secret" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-iam-access-key-secret"
  type  = "SecureString"
  value = data.terraform_remote_state.etl_infrastructure.outputs.nextflow_iam_access_key_secret
}

resource "aws_ssm_parameter" "ecs_iam_role_arn" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-iam-role-arn"
  type  = "String"
  value = data.terraform_remote_state.etl_infrastructure.outputs.ecs_task_iam_role_arn
}

resource "aws_ssm_parameter" "ecs_max_task_retries" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-max-task-retries"
  type  = "String"
  value = var.ecs_max_task_retries
}

resource "aws_ssm_parameter" "ecs_task_container_cpu" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-task-container-cpu"
  type  = "String"
  value = var.ecs_task_container_cpu
}

resource "aws_ssm_parameter" "ecs_task_container_memory" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-task-container-memory"
  type  = "String"
  value = var.ecs_task_container_memory
}

resource "aws_ssm_parameter" "ecs_task_definition" {
  name  = "/${var.environment_name}/${var.service_name}/ecs-task-definition"
  type  = "String"
  value = data.terraform_remote_state.etl_nextflow.outputs.ecs_task_definition_family
}

# Pennsieve API Configuration

resource "aws_ssm_parameter" "api_base_url" {
  name  = "/${var.environment_name}/${var.service_name}/api-base-url"
  type  = "String"
  value = "https://${var.environment_name}-api-${data.terraform_remote_state.region.outputs.aws_region_shortname}.${data.terraform_remote_state.account.outputs.domain_name}"
}

resource "aws_ssm_parameter" "api_queue_size" {
  name  = "/${var.environment_name}/${var.service_name}/api-queue-size"
  type  = "String"
  value = var.api_queue_size
}

resource "aws_ssm_parameter" "api_rate_limit" {
  name  = "/${var.environment_name}/${var.service_name}/api-rate-limit"
  type  = "String"
  value = var.api_rate_limit
}

resource "aws_ssm_parameter" "event_throttle_parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/event-throttle-parallelism"
  type  = "String"
  value = var.event_throttle_parallelism
}

resource "aws_ssm_parameter" "event_throttle_period" {
  name  = "/${var.environment_name}/${var.service_name}/event-throttle-period"
  type  = "String"
  value = var.event_throttle_period
}

# Database Configuration

# DB is model-schema in dev, etl in prod
resource "aws_ssm_parameter" "postgres_database" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-database"
  type  = "String"
  value = var.postgres_database
}

resource "aws_ssm_parameter" "postgres_host" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-host"
  type  = "String"
  value = var.postgres_host
}

# This parameter has been imported.
resource "aws_ssm_parameter" "postgres_password" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-password"
  overwrite = false
  type      = "SecureString"
  value     = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "postgres_user" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-user"
  type  = "String"
  value = "${var.environment_name}_${replace(var.service_name, "-", "_")}_user"
}

# These values must all be the same to prevent transactional deadlock
resource "aws_ssm_parameter" "postgres_min_max_threads_and_connections" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-min-max-threads-and-connections"
  type  = "String"
  value = var.postgres_min_max_threads_and_connections
}

resource "aws_ssm_parameter" "postgres_queue_size" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-queue-size"
  type  = "String"
  value = var.postgres_queue_size
}

# Jobs Handler Configuration

resource "aws_ssm_parameter" "jobs_handler_upload_consumer_sqs_id" {
  name  = "/${var.environment_name}/${var.service_name}/jobs-handler-upload-consumer-sqs-id"
  type  = "String"
  value = data.terraform_remote_state.etl_infrastructure.outputs.uploads_queue_id
}

# Job Scheduler Configuration

resource "aws_ssm_parameter" "job_scheduler_buffer_size" {
  name  = "/${var.environment_name}/${var.service_name}/job-scheduler-buffer-size"
  type  = "String"
  value = var.job_scheduler_buffer_size
}

resource "aws_ssm_parameter" "job_scheduler_max_retries" {
  name  = "/${var.environment_name}/${var.service_name}/job-scheduler-max-retries"
  type  = "String"
  value = var.job_scheduler_max_retries
}

resource "aws_ssm_parameter" "job_scheduler_retry_delay" {
  name  = "/${var.environment_name}/${var.service_name}/job-scheduler-retry-delay"
  type  = "String"
  value = var.job_scheduler_retry_delay
}

resource "aws_ssm_parameter" "job_scheduler_retry_reset_after" {
  name  = "/${var.environment_name}/${var.service_name}/job-scheduler-retry-reset-after"
  type  = "String"
  value = var.job_scheduler_retry_reset_after
}

resource "aws_ssm_parameter" "job_scheduler_timer_run_every" {
  name  = "/${var.environment_name}/${var.service_name}/job-scheduler-timer-run-every"
  type  = "String"
  value = var.job_scheduler_timer_run_every
}

resource "aws_ssm_parameter" "job_scheduler_timer_start_after" {
  name  = "/${var.environment_name}/${var.service_name}/job-scheduler-timer-start-after"
  type  = "String"
  value = var.job_scheduler_timer_start_after
}

# This parameter has been imported.
resource "aws_ssm_parameter" "jwt_secret_key" {
  name  = "/${var.environment_name}/${var.service_name}/jwt-secret-key"
  overwrite = false
  type      = "SecureString"
  value     = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

# Job Pusher Configuration

resource "aws_ssm_parameter" "environment" {
  name  = "/${var.environment_name}/${var.service_name}/environment"
  type  = "String"
  value = var.environment_name
}

resource "aws_ssm_parameter" "pusher_max_tasks" {
  name  = "/${var.environment_name}/${var.service_name}/max-tasks"
  type  = "String"
  value = var.pusher_max_tasks
}

# S3 Configuration (Pusher)

resource "aws_ssm_parameter" "s3_bucket" {
  name  = "/${var.environment_name}/${var.service_name}/s3-bucket"
  type  = "String"
  value = data.terraform_remote_state.etl_infrastructure.outputs.s3_bucket_id
}

resource "aws_ssm_parameter" "s3_host" {
  name  = "/${var.environment_name}/${var.service_name}/s3-host"
  type  = "String"
  value = "https://s3.amazonaws.com"
}

# Watch Dog Configuration

resource "aws_ssm_parameter" "watch_dog_hours_running" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-hours-running"
  type  = "String"
  value = var.watch_dog_hours_running
}

resource "aws_ssm_parameter" "watch_dog_max_retries" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-max-retries"
  type  = "String"
  value = var.watch_dog_max_retries
}

resource "aws_ssm_parameter" "watch_dog_retry_delay" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-retry-delay"
  type  = "String"
  value = var.watch_dog_retry_delay
}

resource "aws_ssm_parameter" "watch_dog_timer_start_after" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-timer-start-after"
  type  = "String"
  value = var.watch_dog_timer_start_after
}

resource "aws_ssm_parameter" "watch_dog_timer_run_every" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-timer-run-every"
  type  = "String"
  value = var.watch_dog_timer_run_every
}

resource "aws_ssm_parameter" "watch_dog_retry_reset_after" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-retry-reset-after"
  type  = "String"
  value = var.watch_dog_retry_reset_after
}

resource "aws_ssm_parameter" "watch_dog_job_ecs_throttle_parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-ecs-throttle-parallelism"
  type  = "String"
  value = var.watch_dog_job_ecs_throttle_parallelism
}

resource "aws_ssm_parameter" "watch_dog_job_ecs_throttle_period" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-ecs-throttle-period"
  type  = "String"
  value = var.watch_dog_job_ecs_throttle_period
}

resource "aws_ssm_parameter" "watch_dog_job_sink_throttle_parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-sink-throttle-parallelism"
  type  = "String"
  value = var.watch_dog_job_sink_throttle_parallelism
}

resource "aws_ssm_parameter" "watch_dog_job_sink_throttle_period" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-sink-throttle-period"
  type  = "String"
  value = var.watch_dog_job_sink_throttle_period
}

resource "aws_ssm_parameter" "watch_dog_job_state_max_retries" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-state-max-retries"
  type  = "String"
  value = var.watch_dog_job_state_max_retries
}

resource "aws_ssm_parameter" "watch_dog_job_state_minutes_stuck" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-state-minutes-stuck"
  type  = "String"
  value = var.watch_dog_job_state_minutes_stuck
}

resource "aws_ssm_parameter" "watch_dog_job_state_throttle_parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-state-throttle-parallelism"
  type  = "String"
  value = var.watch_dog_job_state_throttle_parallelism
}

resource "aws_ssm_parameter" "watch_dog_job_state_throttle_period" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-job-state-throttle-period"
  type  = "String"
  value = var.watch_dog_job_state_throttle_period
}

resource "aws_ssm_parameter" "watch_dog_no_job_throttle_parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-no-job-throttle-parallelism"
  type  = "String"
  value = var.watch_dog_no_job_throttle_parallelism
}

resource "aws_ssm_parameter" "watch_dog_no_job_throttle_period" {
  name  = "/${var.environment_name}/${var.service_name}/watch-dog-no-job-throttle-period"
  type  = "String"
  value = var.watch_dog_no_job_throttle_period
}

// NEW RELIC CONFIGURATION

resource "aws_ssm_parameter" "java_opts" {
  name  = "/${var.environment_name}/${var.service_name}/java-opts"
  type  = "String"
  value = join(" ", local.java_opts)
}

resource "aws_ssm_parameter" "new_relic_app_name" {
  name  = "/${var.environment_name}/${var.service_name}/new-relic-app-name"
  type  = "String"
  value = "${var.environment_name}-${var.service_name}"
}

resource "aws_ssm_parameter" "new_relic_labels" {
  name  = "/${var.environment_name}/${var.service_name}/new-relic-labels"
  type  = "String"
  value = "Environment:${var.environment_name};Service:${local.service};Tier:${local.tier}"
}

resource "aws_ssm_parameter" "new_relic_license_key" {
  name      = "/${var.environment_name}/${var.service_name}/new-relic-license-key"
  overwrite = false
  type      = "SecureString"
  value     = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}
