resource "aws_iam_role" "lambda_iam_role" {
  name = "{{inputs.arn_lambda_role}}"

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
        "Action": ["rds:StartExportTask"],
        "Effect": "Allow",
        "Resource: "*"
    }
  ]
}
EOF
}


# Para o deploy funcionar, gerar um arquivo .zip do arquivo main.py com o nome lambda_function.zip
resource "aws_lambda_function" "lambda_export_snapshot" {
  filename      = "lambda_function.zip"
  function_name = "lambda_export_snapshot"
  role          = aws_iam_role.lambda_iam_role.arn
  handler       = "main.lambda_handler"

  source_code_hash = filebase64sha256("lambda_function.zip")

  runtime = "python3.8"

  environment {
    variables = {
      SUFIXO_SNAPSHOT = // Sufixo do snapshot a ser utilizado
      DBS_SNAPSHOTS = // Dicionario com os nomes dos databases e snapshots
      ROOT_BUCKET_DBS_SNAPSHOTS =  // Pasta raíz (bucket key prefix) dentro do bucket para exportar  o snapshot
      IAM_ROLE_S3_ARN = // ARN da Role IAM com permissão de escrita no bucket S3
    }
  }
}