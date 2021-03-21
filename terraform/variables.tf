variable "aws_account" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "ecs_task_iam_role_id" {}

variable "alarm_namespace" {
  default = "ETL"
}

# SSM Parameters

# Cloudwatch Configuration

variable "cloudwatch_parallelism" {
  default = "10"
}

variable "monitor_max_retries" {
  default = "5"
}

variable "monitor_retry_delay" {
  default = "5 seconds"
}

variable "monitor_retry_reset_after" {
  default = "5 minutes"
}

# ECS Task Configuration

variable "ecs_max_task_retries" {
  default = "2"
}

variable "ecs_task_container_cpu" {
  default = "512"
}

variable "ecs_task_container_memory" {
  default = "1024"
}

# Pennsieve API Configuration

variable "api_queue_size" {
  default = "10000"
}

variable "api_rate_limit" {
  default = "100"
}

variable "event_throttle_parallelism" {
  default = "100"
}

variable "event_throttle_period" {
  default = "1 second"
}

# Database Configurations

variable "postgres_database" {
  default = "etl_postgres"
}

variable "postgres_host" {}

variable "postgres_min_max_threads_and_connections" {
  default = "100"
}

variable "postgres_queue_size" {
  default = "1000"
}

# Job Scheduler Configuration

variable "job_scheduler_buffer_size" {
  default = "100"
}

variable "job_scheduler_timer_run_every" {
  default = "1 second"
}

variable "job_scheduler_max_retries" {
  default = "5"
}

variable "job_scheduler_retry_delay" {
  default = "5 seconds"
}

variable "job_scheduler_retry_reset_after" {
  default = "5 minutes"
}

variable "job_scheduler_timer_start_after" {
  default = "1 second"
}

# Job Pusher Configuration

variable "pusher_max_tasks" {
  default = "70"
}

# Watch Dog Configuration

variable "watch_dog_hours_running" {
  default = "12"
}

variable "watch_dog_max_retries" {
  default = "5"
}

variable "watch_dog_retry_delay" {
  default = "5 seconds"
}

variable "watch_dog_retry_reset_after" {
  default = "5 minutes"
}

variable "watch_dog_timer_start_after" {
  default = "1 second"
}

variable "watch_dog_timer_run_every" {
  default = "10 seconds"
}

variable "watch_dog_job_ecs_throttle_parallelism" {
  default = "100"
}

variable "watch_dog_job_ecs_throttle_period" {
  default = "1 second"
}

variable "watch_dog_job_sink_throttle_parallelism" {
  default = "100"
}

variable "watch_dog_job_sink_throttle_period" {
  default = "1 second"
}

variable "watch_dog_job_state_max_retries" {
  default = "10"
}

variable "watch_dog_job_state_minutes_stuck" {
  default = "30"
}

variable "watch_dog_job_state_throttle_parallelism" {
  default = "20"
}

variable "watch_dog_job_state_throttle_period" {
  default = "1 second"
}

variable "watch_dog_no_job_throttle_parallelism" {
  default = "100"
}

variable "watch_dog_no_job_throttle_period" {
  default = "1 second"
}

# New Relic
variable "newrelic_agent_enabled" {
  default = "true"
}

locals {
  java_opts = [
    "-javaagent:/app/newrelic.jar",
    "-Dnewrelic.config.agent_enabled=${var.newrelic_agent_enabled}",
  ]

  service = element(split("-", var.service_name), 0)
  tier    = element(split("-", var.service_name), 1)
}

