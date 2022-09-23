from datetime import datetime
import logging 
import json 
import boto3
import botocore
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info(event)
    SUFIXO_SNAPSHOT = get_environment_variable('SUFIXO_SNAPSHOT')
    DBS_SNAPSHOTS = get_environment_variable('DBS_SNAPSHOTS')
    DBS_SNAPSHOTS = DBS_SNAPSHOTS.split(";")
    DBS_SNAPSHOTS = list(
        map(
            lambda db_instance_identifier: db_instance_identifier + SUFIXO_SNAPSHOT, DBS_SNAPSHOTS
        )
    )

    ROOT_BUCKET_DBS_SNAPSHOTS = get_environment_variable(
        'ROOT_BUCKET_DBS_SNAPSHOTS'
        )
    IAM_ROLE_S3_ARN = get_environment_variable('IAM_ROLE_S3_ARN')


    list_snapshot = iterator_snapshot(event)

    logger.info(f"Snapshots para iniciar: {list_snapshot}")

    for event in list_snapshot:

        detail = event['detail']

        if detail['SourceIdentifier'] in DBS_SNAPSHOTS:
            bucket_export_snapshot = get_bucket_db_snapshot(
                detail['SourceIdentifier']
            )

            create_object_s3(
                bucket_export_snapshot,
                get_objects_bucket(ROOT_BUCKET_DBS_SNAPSHOTS),
                ROOT_BUCKET_DBS_SNAPSHOTS
            )

            export_snapshot_s3(
                detail['SourceIdentifier'],
                detail['SourceArn'],
                ROOT_BUCKET_DBS_SNAPSHOTS,
                bucket_export_snapshot,
                IAM_ROLE_S3_ARN
            )

            message = _message_return(
                detail=detail,
                bucket_export_snapshot=bucket_export_snapshot,
                root_bucket_dbs_snapshots=ROOT_BUCKET_DBS_SNAPSHOTS
            )
            return message

def _message_return(detail,bucket_export_snapshot,root_bucket_dbs_snapshots):

    now = datetime.now()

    dict = {
        'snapshot': detail['SourceIdentifier'],
        'bucket': root_bucket_dbs_snapshots,
        's3prefix': bucket_export_snapshot,
        'datetime': now.strftime("%d/%m/%Y %H:%M:%S"),
        'statusCode': 200
    }

    return dict

def iterator_snapshot(event):

    list = []

    if event.get("Records", False):
        logger.info("Origem evento SQS!")
        for x in event.get("Records"):
            payload = json.loads(json.loads(
                x.get('body')).get("Message"))
            list.append(payload) 
            
        return list 

    return [event]

def get_bucket_db_snapshot(snapshot_db_identifier): 
    BUCKETS_DBS_SNAPSHOTS = get_environment_variable('BUCKETS_DBS_SNAPSHOTS') 
    
    try: 
        logger.info(
            "INFO: Obtendo o bucket para exportação do snapshot - \
            Snapshot: {}".format(snapshot_db_identifier)
            )
        BUCKETS_DBS_SNAPSHOTS = json.loads(BUCKETS_DBS_SNAPSHOTS) 
        bucket_snapshot = BUCKETS_DBS_SNAPSHOTS[snapshot_db_identifier] 
        logger.info(
            "INFO: Snapshot: {} | Bucket: {}" 
            .format(snapshot_db_identifier, bucket_snapshot)
            )
        return bucket_snapshot 
        
    except ValueError: 
        raise ValueError(
            "ERROR: Erro ao carregar JSON da variável de \
                ambiente BUCKETS_DBS_SNAPSHOTS"
                ) 
        
    except KeyError:
        raise KeyError(
            "ERROR: Não foi encontrado o bucket para exportação do snapshot - \
            Snapshot: {} | Bucket: ?".format(snapshot_db_identifier))


rds = boto3.client('rds')
now = datetime.now()

def export_snapshot_s3( 
    snapshot_db_identifier, 
    snapshot_db_arn, 
    root_diretory, 
    bucket_export_snapshot, 
    i_am_role_s3_arn
    ): 
    
    try: 
        logger.info(
            "INFO: Iniciada a exportação do snapshot para o S3 - \
            Snapshot: {} | Bucket: {}" 
            .format( snapshot_db_identifier, 
            root_diretory + '/' + bucket_export_snapshot
            )
        )

        id_snapshot = snapshot_db_identifier + '-' + now.strftime("%d%m%Y")
            
        export_status = rds.start_export_task( 
            ExportTaskIdentifier=id_snapshot, 
            SourceArn=snapshot_db_arn, 
            S3BucketName=root_diretory, 
            S3Prefix=bucket_export_snapshot[:-1], 
            IamRoleArn=i_am_role_s3_arn,
            )

    
        return {
            'ExportTaskIdentifier': export_status['ExportTaskIdentifier'],
            'SourceArn': export_status['SourceArn'], 
            'S3Bucket': export_status['S3Bucket'], 
            'S3Prefix': export_status['S3Prefix'], 
            'Status': export_status['Status'], 
            'ResponseMetadata': { 
                'RequestId': export_status['ResponseMetadata']['RequestId'], 
                'HTTPStatusCode': export_status['ResponseMetadata'] ['HTTPStatusCode'] 
                } 
            } 
    except botocore.exceptions.ClientError as error:
        if (error.response['Error']['Code'] == 'ExportTaskLimitReachedFault'):
            logger.error(error) 
    except Exception as e:
         logger.error(e) 
         logger.error( "ERROR: Erro ao exportar o snapshot para o S3 - \
             Snapshot: {} | Bucket: {}" .format( snapshot_db_identifier, root_diretory + '/' + bucket_export_snapshot 
            ) 
        )


def create_object_s3(
        bucket_db_snapshot,
        objects_bucket,
        root_bucket_dbs_snapshots):
    try:
        creation_status = {}
        if len(objects_bucket) == 0 \
                or bucket_db_snapshot not in objects_bucket:
            logger.info(
                "INFO: Criando estrutura S3 para exportação do snapshot - \
                Bucket: {} | Estrutura: {}"
                .format(root_bucket_dbs_snapshots, bucket_db_snapshot)
            )
            response = s3.put_object(
                Bucket=root_bucket_dbs_snapshots, Key=bucket_db_snapshot
            )
            logger.info(
                "INFO: Objeto criado no S3 - \
                Objeto: {} | Bucket: {}"
                .format(bucket_db_snapshot, root_bucket_dbs_snapshots)
            )
            logger.info(
                "INFO: Response da criação do objeto: {}"
                .format(response)
            )
            creation_status = {
                'Object': bucket_db_snapshot,
                'HTTPStatusCode':
                    response['ResponseMetadata']['HTTPStatusCode']
            }
        return creation_status
    except Exception as e:
        logger.error(e)
        logger.error(
            "ERROR: Não foi possível criar o objeto no bucket - \
            Bucket: {}".format(root_bucket_dbs_snapshots)
        )


def get_objects_bucket(bucket):
    try:
        logger.info(
            "INFO: Obtendo objetos do bucket - \
            Bucket: {}".format(bucket)
        )
        objects_bucket = s3.list_objects_v2(
            Bucket=bucket,
            Delimiter='vsnapshot').get('Contents')

        if objects_bucket is not None:
            object_keys = [obj.get('Key') for obj in objects_bucket]
            logger.info(
                "INFO: Objetos encontrados no bucket - \
                Objects: {}".format(object_keys)
            )
            return object_keys
        else:
            logger.info(
                "INFO: Nenhum objeto foi encontrado no bucket - \
                Bucket: {}".format(bucket)
            )
            return []
    except Exception as e:
        logger.error(e)
        logger.error(
            "ERROR: Bucket não existe - \
            Bucket: {}".format(bucket)
        )


def get_environment_variable(environment_variable_name):
    logger.setLevel(logging.INFO)
    try:
        logger.info(
            "INFO: Obtendo valor da variável de ambiente {}"
            .format(environment_variable_name)
        )
        environment_variable_value = os.environ[environment_variable_name]
        return environment_variable_value
    except Exception as e:
        logger.error(e)
        logger.error(
            "ERROR: A variável de ambiente {} não existe"
            .format(environment_variable_name)
        )
