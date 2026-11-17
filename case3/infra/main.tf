# Serverless half of the target architecture. Redis, Postgres and the services run locally
# (docker compose). Everything is named and tagged poc-dennis.
#
#   terraform init && terraform apply      # uses the AWS credentials in your environment
#   terraform destroy      # removes everything, including the bucket contents

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.80" }
    archive = { source = "hashicorp/archive", version = "~> 2.6" }
    random  = { source = "hashicorp/random", version = "~> 3.6" }
    local   = { source = "hashicorp/local", version = "~> 2.5" }
  }
}

variable "region" {
  type    = string
  default = "ap-northeast-1"
}

variable "name" {
  type    = string
  default = "poc-dennis"
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.name, ManagedBy = "terraform" }
  }
}

data "aws_caller_identity" "me" {}

locals {
  bucket   = "${var.name}-lake-${data.aws_caller_identity.me.account_id}"
  database = replace(var.name, "-", "_")
}

# ---------------------------------------------------------------- stream
# One shard carries the assumed peak of 300 events per second with room to spare.
resource "aws_kinesis_stream" "events" {
  name             = "${var.name}-events"
  shard_count      = 1
  retention_period = 24
  stream_mode_details { stream_mode = "PROVISIONED" }
}

# ---------------------------------------------------------------- data lake
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

# ---------------------------------------------------------------- de-identification
# The same file the local code imports, deployed as the Firehose transform.
resource "random_password" "deid_secret" {
  length  = 32
  special = false
}

data "archive_file" "deid" {
  type        = "zip"
  source_file = "${path.module}/../target_arch/deid.py"
  output_path = "${path.module}/.build/deid.zip"
}

resource "aws_iam_role" "deid" {
  name = "${var.name}-deidentify"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "deid_logs" {
  role       = aws_iam_role.deid.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "deid" {
  function_name    = "${var.name}-deidentify"
  role             = aws_iam_role.deid.arn
  runtime          = "python3.12"
  handler          = "deid.handler"
  filename         = data.archive_file.deid.output_path
  source_code_hash = data.archive_file.deid.output_base64sha256
  timeout          = 60
  memory_size      = 128
  environment {
    variables = { DEID_SECRET = random_password.deid_secret.result }
  }
}

# ---------------------------------------------------------------- firehose
resource "aws_iam_role" "firehose" {
  name = "${var.name}-firehose"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "firehose.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"]
        Resource = aws_kinesis_stream.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
        Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"]
        Resource = "${aws_lambda_function.deid.arn}:*"
      },
    ]
  })
}

resource "aws_kinesis_firehose_delivery_stream" "lake" {
  name        = "${var.name}-to-lake"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.lake.arn
    prefix              = "events/dt=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"
    buffering_interval  = 60
    buffering_size      = 1

    processing_configuration {
      enabled = true
      processors {
        type = "Lambda"
        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.deid.arn}:$LATEST"
        }
      }
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
}

# ---------------------------------------------------------------- athena
resource "aws_glue_catalog_database" "lake" {
  name = local.database
}

resource "aws_glue_catalog_table" "events" {
  name          = "events"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "projection.enabled"        = "true"
    "projection.dt.type"        = "date"
    "projection.dt.format"      = "yyyy-MM-dd"
    "projection.dt.range"       = "2026-11-01,NOW"
    "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/events/dt=$${dt}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/events/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "user_key"
      type = "string"
    }
    columns {
      name = "type"
      type = "string"
    }
    columns {
      name = "ts"
      type = "double"
    }
    columns {
      name = "payload"
      type = "string"
    }
  }
}

resource "aws_athena_workgroup" "lake" {
  name          = var.name
  force_destroy = true
  configuration {
    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB per query
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}

# ---------------------------------------------------------------- scheduler
# One-off wake-ups (the 10-minute buffer) are created at run time in this group.
resource "aws_sqs_queue" "triggers" {
  name                       = "${var.name}-triggers"
  message_retention_seconds  = 86400
  visibility_timeout_seconds = 60
}

resource "aws_scheduler_schedule_group" "triggers" {
  name = var.name
}

resource "aws_iam_role" "scheduler" {
  name = "${var.name}-scheduler"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "scheduler.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sqs:SendMessage", Resource = aws_sqs_queue.triggers.arn }]
  })
}

# Schedule-type trigger: the daily churn scan, 10:00 Taipei time.
resource "aws_scheduler_schedule" "churn_scan" {
  name                         = "${var.name}-daily-churn-scan"
  group_name                   = aws_scheduler_schedule_group.triggers.name
  schedule_expression          = "cron(0 10 * * ? *)"
  schedule_expression_timezone = "Asia/Taipei"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_sqs_queue.triggers.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ type = "churn_scan" })
  }
}

# ---------------------------------------------------------------- handoff to the local services
resource "local_sensitive_file" "env" {
  filename        = "${path.module}/../.env.aws"
  file_permission = "0600"
  content         = <<-EOT
    AWS_REGION=${var.region}
    AWS_DEFAULT_REGION=${var.region}
    KINESIS_STREAM=${aws_kinesis_stream.events.name}
    ATHENA_WORKGROUP=${aws_athena_workgroup.lake.name}
    ATHENA_DATABASE=${aws_glue_catalog_database.lake.name}
    TRIGGER_QUEUE_URL=${aws_sqs_queue.triggers.url}
    TRIGGER_QUEUE_ARN=${aws_sqs_queue.triggers.arn}
    SCHEDULER_GROUP=${aws_scheduler_schedule_group.triggers.name}
    SCHEDULER_ROLE_ARN=${aws_iam_role.scheduler.arn}
    DEID_SECRET=${random_password.deid_secret.result}
  EOT
}

output "resources" {
  value = {
    kinesis_stream = aws_kinesis_stream.events.name
    firehose       = aws_kinesis_firehose_delivery_stream.lake.name
    lambda         = aws_lambda_function.deid.function_name
    bucket         = aws_s3_bucket.lake.bucket
    glue_table     = "${aws_glue_catalog_database.lake.name}.events"
    athena         = aws_athena_workgroup.lake.name
    queue          = aws_sqs_queue.triggers.name
    schedule_group = aws_scheduler_schedule_group.triggers.name
    daily_schedule = aws_scheduler_schedule.churn_scan.name
  }
}
