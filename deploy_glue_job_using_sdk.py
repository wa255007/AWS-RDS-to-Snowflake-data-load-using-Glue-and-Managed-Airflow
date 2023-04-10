import logging
import time
import boto3
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('glue')
s3_client= boto3.client('s3')

def copy_script_to_s3(script_path,bucket, bucket_key):
    filename=script_path.split("\\")[-1].replace(".py","")
    logger.info (f"file name read and deployed name will be {filename}")
    key= f"{bucket_key}/{filename}.py"
    res= s3_client.put_object (Body=open(script_path,'rb'), Bucket=bucket,Key=key,SSEKMSKeyId='224b3b76-85f8-426a-9a76-5fc74289cb3d',ServerSideEncryption ='aws:kms')
    bucket_key=f"s3://{bucket}/{key}"
    return filename, bucket_key

def copy_config_to_s3(script_path,bucket, bucket_key):
    filename=script_path.split("\\")[-1]
    logger.info (f"file name read and deployed name will be {filename}")
    key= f"{bucket_key}/{filename}"
    res= s3_client.put_object (Body=open(script_path,'rb'), Bucket=bucket,Key=key,SSEKMSKeyId='224b3b76-85f8-426a-9a76-5fc74289cb3d',ServerSideEncryption ='aws:kms')
    bucket_key=f"s3://{bucket}/{key}"
    return filename, bucket_key

def create_glue_job(glue_job_name,role,vpc_connection,script_location):
    logging.info(f"received parameters are {glue_job_name},{role} , {script_location}")
    job = client.create_job(Name=glue_job_name, Role=role,GlueVersion='3.0',
                          Command={'Name': 'glueetl',
                                   'ScriptLocation': script_location}
                            ,ExecutionProperty={
                                'MaxConcurrentRuns': 50
                            },Connections={
        'Connections': [
            vpc_connection,
        ] }
                            ,NumberOfWorkers=5
                            ,WorkerType='G.1X'
                           )
    logging.info(f"job created successfully")
    return job


if __name__=="__main__":
    start_time = time.time()
    #load config file parameters
    with open("rds_snowflake_config.json") as f:
        map=f.read()
        config=json.loads(map)
    glue_job_name,s3_path_key = copy_script_to_s3(config["script_path"],config["bucket"], config["bucket_key"])
    config_name, s3_path_key_config = copy_config_to_s3(config["config_path"], config["bucket"], config["bucket_key"])
    response=create_glue_job(glue_job_name, config["role"],config["glue_vpc_connector"], s3_path_key)
    print ("here is the deployed response", response)
    logging.info(f"Job completed successfully")