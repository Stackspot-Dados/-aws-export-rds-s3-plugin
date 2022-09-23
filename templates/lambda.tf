resource "aws_iam_role" "lambda_iam_role" {
  name = "{{inputs.lambda_role}}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    },
    {
        "Action": ["glue:StartWorkflowRun", "glue:PutWorkflowRunProperties"],
        "Effect": "Allow",
        "Resource: "*"
    }
  ]
}
EOF
}


# Para o deploy funcionar, gerar um arquivo .zip do arquivo main.py com o nome lambda_function.zip
resource "aws_lambda_function" "lambda_start_workflow" {
  filename      = "lambda_function.zip"
  function_name = "lambda_start_workflow"
  role          = aws_iam_role.lambda_iam_role.arn
  handler       = "main.lambda_handler"

  source_code_hash = filebase64sha256("lambda_function.zip")

  runtime = "python3.8"

  environment {
    variables = {
      WORKFLOW_NAME = {{inputs.workflow_name}}
      DIC_BUCKETS_DBS = {"bucket/snapshots/teste/":"DatabseGlue"} // Dicionario com os nomes dos buckets e Databases do Glue
    }
  }
}