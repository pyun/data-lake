import sys
import boto3


client = boto3.client('logs')
response = client.describe_log_groups(
    logGroupNamePrefix='/ecs/mooc-server-teacher-def'
)
print(response)


response1 = client.create_export_task(
    taskName='testtask1111',
    logGroupName='/ecs/mooc-server-teacher-def',
    fromTime=1661356800000,
    to=1661443200000,
    destination='cloudwatchlog-test-py',
    destinationPrefix='glueexportout'
)
