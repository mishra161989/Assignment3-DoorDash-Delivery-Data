import boto3
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-east-2:992382818239:Doordash-daily-feed-processing'


def lambda_handler(event, context):
    try:
        output_bucket = 'hk-doordash-target-zn'
        # Get the s3 bucket and object key from the event trigger.
        print(event)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        # Use boto3 to get the json file from s3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        print("printing response : ", response)
        file_content = response['Body'].read().decode('utf-8')

        df = pd.read_json(file_content, lines=True)
        final_df = df[df['status'] == 'delivered']
        final_df = final_df.copy()
        final_df.loc[:, 'date'] = final_df['date'].astype(str)
        print("printing final_df: ", final_df)

        input_key = key.split('-')[0:3]
        out_key = '-'.join(input_key) + '-filtered_data.json'

        output_content = final_df.to_json(orient='records', lines=True)
        s3_client.put_object(Bucket=output_bucket, Key=out_key, Body=output_content)

        message = "Input Doordash S3 File {} has been processed successfully !!".format("s3://"+bucket+"/"+key)
        response = sns_client.publish(Subject="SUCCESS - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')

    except Exception as err:
        print(err)
        message = "Input Doordash S3 File {} processing is Failed !!".format("s3://"+bucket+"/"+key)
        response = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')
