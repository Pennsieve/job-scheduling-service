resource "aws_iam_policy" "iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/service/"
  policy = data.aws_iam_policy_document.iam_policy_document.json
}

resource "aws_iam_role_policy_attachment" "iam_role_policy_attachment" {
  role       = var.ecs_task_iam_role_id
  policy_arn = aws_iam_policy.iam_policy.arn
}

data "aws_iam_policy_document" "iam_policy_document" {
  statement {
    sid       = "ECSListPermissions"
    effect    = "Allow"
    actions   = ["ecs:ListTasks"]
    resources = ["*"]
  }

  statement {
    sid    = "ECSTaskPermissions"
    effect = "Allow"

    actions = [
      "ecs:RunTask",
      "ecs:StopTask",
    ]

    resources = ["arn:aws:ecs:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:task-definition/${data.terraform_remote_state.etl_nextflow.outputs.ecs_task_definition_family}"]
  }

  statement {
    sid       = "IAMPassRole"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = ["*"]
  }

  statement {
    sid    = "KMSPermissions"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.notifications_kms_key_arn,
      data.terraform_remote_state.etl_infrastructure.outputs.sqs_kms_key_arn,
    ]
  }

  statement {
    sid    = "SQSSendPermissions"
    effect = "Allow"

    actions = [
      "sqs:GetQueueAttributes",
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.notifications_queue_arn,
      data.terraform_remote_state.etl_infrastructure.outputs.uploads_queue_arn,
    ]
  }

  statement {
    sid    = "SQSDeleteReceivePermissions"
    effect = "Allow"

    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessageBatch",
      "sqs:DeleteMessage",
    ]

    resources = [data.terraform_remote_state.etl_infrastructure.outputs.nextflow_status_queue_arn]
  }

  statement {
    sid     = "S3Permissions"
    effect  = "Allow"
    actions = ["s3:*"]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.uploads_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.uploads_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn}/*",
      data.terraform_remote_state.etl_infrastructure.outputs.s3_bucket_arn,
      "${data.terraform_remote_state.etl_infrastructure.outputs.s3_bucket_arn}/*",
    ]
  }

  # SSM Permissions
  statement {
    sid       = "KMSDecryptPermissions"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["arn:aws:kms:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:key/alias/aws/ssm"]
  }

  statement {
    sid       = "SecretsManagerListPermissions"
    effect    = "Allow"
    actions   = ["secretsmanager:ListSecrets"]
    resources = ["*"]
  }

  statement {
    sid    = "SSMPermissions"
    effect = "Allow"

    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]

    resources = ["arn:aws:ssm:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.environment_name}/${var.service_name}/*"]
  }
}
