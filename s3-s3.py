import sys
import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType, BooleanType, TimestampType
from awsglue.context import GlueContext
from awsglue.job import Job




def split_fixed_previous_time(fixed_num):
    current_time = datetime.datetime.now()
    previous_time = (current_time - datetime.timedelta(days=fixed_num)).strftime('%Y-%m-%d %H:%M:%S')
    split_arr = previous_time.split(' ')
    date_arr = split_arr[0].split('-')
    time_arr = split_arr[1].split(':')
    return date_arr[0], date_arr[1], date_arr[2], time_arr[0], time_arr[1], time_arr[2]




#job_name = 'sink_ods_web_pay_cashier'
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


args = getResolvedOptions(sys.argv, ["job_name"])
previous_time_arr = split_fixed_previous_time(1)
p_year = previous_time_arr[0]
p_month = previous_time_arr[1]
p_day = previous_time_arr[2]
p_hour = previous_time_arr[3]




web_pay_cashier_schema = StructType(
    fields=[
        StructField('appId', StringType()),
        StructField('appname', StringType()),
        StructField('beylaId', StringType()),
        StructField('clientIp', StringType()),
        StructField('deviceInfo', StructType([
            StructField('browserType', StringType()),
            StructField('browserVersion', StringType()),
            StructField('dpr', DoubleType()),
            StructField('language', StringType()),
            StructField('manufacturer', StringType()),
            StructField('mobileModel', StringType()),
            StructField('netStatus', StringType()),
            StructField('osType', StringType()),
            StructField('osVersion', StringType()),
            StructField('platform', StringType()),
            StructField('screenHeight', IntegerType()),
            StructField('screenWidth', IntegerType()),
            StructField('timeZone', IntegerType()),
            StructField('userAgent', StringType()),
        ])),
        StructField('fingerprint', StringType()),
        StructField('groupname', StringType()),
        StructField('logStore', StringType()),
        StructField('params', StructType([
            StructField('eventName', StringType()),
            StructField('extras', StructType([
                StructField('$is_login_id', BooleanType()),
                StructField('__cp_location', StringType()),
                StructField('_time', TimestampType()),
                StructField('account_no', StringType()),
                StructField('api', StringType()),
                StructField('card_num_length', IntegerType()),
                StructField('card_org', StringType()),
                StructField('card_prefix', StringType()),
                StructField('code', StringType()),
                StructField('component_type', StringType()),
                StructField('components', IntegerType()),
                StructField('cost_time', TimestampType()),
                StructField('deep_link', IntegerType()),
                StructField('display_code', StringType()),
                StructField('duration', LongType()),
                StructField('error_msg', StringType()),
                StructField('event_type', StringType()),
                StructField('extra_params', StructType([
                    StructField('component_type', StringType()),
                    StructField('payment_method', StringType()),
                ])),
                StructField('full_path', StringType()),
                StructField('info', StringType()),
                StructField('is_designated', BooleanType()),
                StructField('is_recommend', BooleanType()),
                StructField('loading_time', TimestampType()),
                StructField('location', StringType()),
                StructField('method', StringType()),
                StructField('msg', StringType()),
                StructField('name', StringType()),
                StructField('parent_payment', StringType()),
                StructField('payment_flow', StringType()),
                StructField('payment_method', StringType()),
                StructField('payment_method_type', StringType()),
                StructField('payment_mode', StringType()),
                StructField('portal', StringType()),
                StructField('report_time', TimestampType()),
                StructField('request_data', StringType()),
                StructField('request_time', TimestampType()),
                StructField('request_url', StringType()),
                StructField('response_data', StringType()),
                StructField('response_time', TimestampType()),
                StructField('select_country', StringType()),
                StructField('select_language', StringType()),
                StructField('state', StringType()),
                StructField('status', IntegerType()),
                StructField('status_text', StringType()),
                StructField('tab', StringType()),
                StructField('target_org', StringType()),
                StructField('url', StringType()),
            ])),
            StructField('pveCur', StringType()),
        ])),
        StructField('payed', StringType()),
        StructField('performance',  ArrayType(
            StructType([
                StructField('memory', StructType([
                    StructField('jsHeapSizeLimit', LongType()),
                    StructField('totalJSHeapSize', LongType()),
                    StructField('usedJSHeapSize', LongType()),
                ])),
                StructField('paintTiming', StructType([
                    StructField('fcp', DoubleType()),
                    StructField('fp', DoubleType()),
                ])),
                StructField('resourceTiming', StructType([
                    StructField('connect', LongType()),
                    StructField('duration', LongType()),
                    StructField('entryType', StringType()),
                    StructField('initiatorType', StringType()),
                    StructField('lookupDomain', LongType()),
                    StructField('name', LongType()),
                    StructField('nextHopProtocol', LongType()),
                    StructField('redirect', LongType()),
                    StructField('request', LongType()),
                ])),
                StructField('timing', StructType([
                    StructField('appcache', LongType()),
                    StructField('connect', LongType()),
                    StructField('domReady', LongType()),
                    StructField('loadEvent', LongType()),
                    StructField('loadPage', LongType()),
                    StructField('lookupDomain', LongType()),
                    StructField('redirect', LongType()),
                    StructField('request', LongType()),
                    StructField('ttfb', LongType()),
                    StructField('unloadEvent', LongType()),
                ])),
            ])
        )),
        StructField('publicParams',  StructType([
                StructField('_user_id', StringType()),
                StructField('country', StringType()),
                StructField('device_os', StringType()),
                StructField('device_type', StringType()),
                StructField('env', StringType()),
                StructField('host', StringType()),
                StructField('language', StringType()),
                StructField('merchant_app_id', StringType()),
                StructField('merchant_id', StringType()),
                StructField('orientation', StringType()),
                StructField('os_type', StringType()),
                StructField('out_trade_no', StringType()),
                StructField('project', StringType()),
                StructField('source', StringType()),
                StructField('terminal_type', StringType()),
                StructField('trade_token', StringType()),
                StructField('url', StringType()),
                StructField('user_id', StringType()),
        ])),
        StructField('region', StringType()),
        StructField('reportTime', LongType()),
        StructField('reportType', StringType()),
        StructField('url', StringType()),
    ]
)


print('s3://cloudwatchlog-test-py/payer/' + p_year + '/' + p_month + '/01/00')
df = spark.read.option("recursiveFileLookup", "true").json(
    path='s3://cloudwatchlog-test-py/payer/' + p_year + '/' + p_month + '/',
    schema=web_pay_cashier_schema
)




#df.write.partitionBy("gender","salary").mode("overwrite").parquet("/tmp/output/people2.parquet")


df.repartition(1).write.mode('overwrite').parquet(
    path='s3://cloudwatchlog-test-py/payerout/dt=' + p_year + '-' + p_month + '-' + p_day + '/'
)


job = Job(glueContext)
job.init(args["job_name"], args)
job.commit()
