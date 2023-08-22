#
# This Terraform file is necessary to configure the basic
# infrastructure around the Lifecycler function

resource "aws_lambda_function" "lambda" {
  description   = "A simple lambda for using lifecycle policies with a Delta table"
  filename      = "target/lambda/lambda-delta-lifecycler/bootstrap.zip"
  function_name = "delta-lifecycler"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "provided"
  runtime       = "provided.al2"

  environment {
    variables = {
      AWS_S3_LOCKING_PROVIDER = "dynamodb"
      DYNAMO_LOCK_TABLE_NAME  = "delta-lifecycler"
      DATALAKE_LOCATION       = "s3://${aws_s3_bucket.lifecycler.id}/databases/example"
      RUST_LOG                = "debug"
    }
  }
}

resource "aws_s3_bucket" "lifecycler" {
  bucket = "delta-lifecycler"
}
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.lifecycler.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda.arn
    events              = ["s3:ObjectRemoved:*"]
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_function.lambda]
}

variable "aws_access_key" {
  type    = string
  default = ""
}

variable "aws_secret_key" {
  type    = string
  default = ""
}

provider "aws" {
  region     = "us-west-2"
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key

  default_tags {
    tags = {
      ManagedBy   = "Terraform"
      environment = terraform.workspace
      workspace   = terraform.workspace
    }
  }
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
}

resource "aws_iam_policy" "lambda_permissions" {
  name = "lambda-delta-lifecycler-permissions"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["dynamodb:*"]
        Resource = aws_dynamodb_table.delta-locking-table.arn
        Effect   = "Allow"
      },
      {
        Action   = ["s3:*"]
        Resource = aws_s3_bucket.lifecycler.arn
        Effect   = "Allow"
      }
    ]
  })
}

resource "aws_iam_role" "iam_for_lambda" {
  name                = "iam_for_lifecycler_lambda"
  assume_role_policy  = data.aws_iam_policy_document.assume_role.json
  managed_policy_arns = [aws_iam_policy.lambda_permissions.arn]
}

resource "aws_dynamodb_table" "delta-locking-table" {
  name         = "delta-lifecycler"
  billing_mode = "PROVISIONED"
  # Default name of the partition key hard-coded in delta-rs
  hash_key       = "key"
  read_capacity  = 10
  write_capacity = 10

  attribute {
    name = "key"
    type = "S"
  }
}
