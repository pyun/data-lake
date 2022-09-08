import sys
import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import split, col, to_json,from_json
from awsglue.context import GlueContext
from pyspark.sql.types import StructType, StructField, StringType, LongType
from awsglue.job import Job

def split_fixed_previous_time(fixed_num):
    current_time = datetime.datetime.now()
    previous_time = (current_time - datetime.timedelta(days=fixed_num)).strftime('%Y-%m-%d %H:%M:%S')
    split_arr = previous_time.split(' ')
    date_arr = split_arr[0].split('-')
    time_arr = split_arr[1].split(':')
    return date_arr[0], date_arr[1], date_arr[2], time_arr[0], time_arr[1], time_arr[2]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
previous_time_arr = split_fixed_previous_time(1)
p_year = previous_time_arr[0]
p_month = previous_time_arr[1]
p_day = previous_time_arr[2]

merchant_gateway_schema = StructType(
    fields=[
        StructField('apiInfo', StructType([
            StructField('api', StringType()),
            StructField('clientType', StringType()),
            StructField('direction', StringType()),
            StructField('endTime', StringType()),
            StructField('flowType', StringType()),
            StructField('protocol', StringType()),
            StructField('startTime', StringType()),
            StructField('version', StringType()),
        ])),
        StructField('businessInfo', StructType([
            StructField('ReqCardOrg', StringType()),
            StructField('ReqCountry', StringType()),
            StructField('ReqCurrency', StringType()),
            StructField('ReqMerchantNo', StringType()),
            StructField('ReqPayCurrency', StringType()),
            StructField('ReqPayRequestNo', StringType()),
            StructField('ReqPaymentFlow', StringType()),
            StructField('ReqPaymentMethodType', StringType()),
            StructField('ReqPaymentMode', StringType()),
            StructField('ReqTargetOrg', StringType()),
            StructField('ReqTradeToken', StringType()),
            StructField('acceptId', StringType()),
            StructField('acceptStatus', StringType()),
            StructField('apiSpec', StringType()),
            StructField('appId', StringType()),
            StructField('bizCode', StringType()),
            StructField('bizIdentify', StringType()),
            StructField('bizType', StringType()),
            StructField('channelCode', StringType()),
            StructField('channelOrderNo', StringType()),
            StructField('channelPayRequestNo', StringType()),
            StructField('country', StringType()),
            StructField('currency', StringType()),
            StructField('dbPair', StringType()),
            StructField('filterRouteInfo', StructType([
                StructField('AmountLimitFilterRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('ExtendPropertyFilterRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('LimitRouteRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('ParamsFilterRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('PriorityWeightRouteRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('SpecialListFilterRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('SpecialListRouteRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
                StructField('TargetMerchantFilterRule', StructType([
                    StructField('filteredChannels', StructType([
                        StructField('code', StringType()),
                        StructField('condition', StringType()),
                        StructField('reason', StringType()),
                    ])),
                    StructField('order', StringType()),
                    StructField('remainChannels', StringType()),
                ])),
            ])),
            StructField('fxTradeErrorCode', StringType()),
            StructField('hasHttpCopy', StringType()),
            StructField('invokeDSFlag', StringType()),
            StructField('memberId', StringType()),
            StructField('merchantLevel', StringType()),
            StructField('merchantName', StringType()),
            StructField('merchantNo', StringType()),
            StructField('methodCode', StringType()),
            StructField('methodSubCode', StringType()),
            StructField('money', StringType()),
            StructField('newMerchantNo', StringType()),
            StructField('oldMerchantNo', StringType()),
            StructField('ordinaryLevel', StringType()),
            StructField('outTradeNo', StringType()),
            StructField('payNo', StringType()),
            StructField('productCode', StringType()),
            StructField('respCode', StringType()),
            StructField('respMsg', StringType()),
            StructField('respResultType', StringType()),
            StructField('seriousLevel', StringType()),
            StructField('targetOrg', StringType()),
            StructField('tradeNo', StringType()),
            StructField('transCode', StringType()),
            StructField('transactionNo', StringType()),
            StructField('useSharding', StringType()),
        ])),
        StructField('env', StructType([
            StructField('localEnv', StringType()),
            StructField('localIp', StringType()),
            StructField('sysName', StringType()),
        ])),
        StructField('identityInfo', StructType([
            StructField('sourceId', StringType()),
            StructField('sourceIp', StringType()),
            StructField('sourceSys', StringType()),
            StructField('targetIp', StringType()),
            StructField('targetOuterFlag', StringType()),
            StructField('targetSys', StringType()),
        ])),
        StructField('request', StructType([
            StructField('body', StringType()),
            StructField('httpMethod', StringType()),
            StructField('networkCostTime', LongType()),
            StructField('reqUrl', StringType()),
            StructField('user_agent', StructType([
                StructField('device', StructType([
                    StructField('name', StringType()),
                ])),
                StructField('name', StringType()),
                StructField('original', StringType()),
                StructField('os', StructType([
                    StructField('full', StringType()),
                    StructField('name', StringType()),
                    StructField('version', StringType()),
                ])),
                StructField('version', StringType()),
            ])),
        ])),
        StructField('response', StructType([
            StructField('body', StringType()),
            StructField('resultInfo', StructType([
                StructField('code', StringType()),
                StructField('codeName', StringType()),
                StructField('costTime', LongType()),
                StructField('exceptionName', StringType()),
                StructField('httpStatusCode', StringType()),
                StructField('message', StringType()),
                StructField('status', StringType()),
                StructField('timeoutFlag', StringType()),
            ])),
        ])),
        StructField('traceContextInfo', StructType([
            StructField('BIZ_PRINCIPAL', StringType()),
            StructField('CHANNEL_CODE', StringType()),
            StructField('CHANNEL_ORDER_NO', StringType()),
            StructField('PAYMENT_METHOD', StringType()),
            StructField('PAYMENT_METHOD_TYPE', StringType()),
            StructField('SOURCE_APP_NAME', StringType()),
            StructField('SOURCE_APP_REQUEST_TIME', StringType()),
            StructField('TARGET_ORG', StringType()),
        ])),
        StructField('type', StringType()),
    ]
)

log_df = spark.read\
    .option('recursiveFileLookup', 'true')\
    .option('header', 'false')\
    .option("inferSchema", "true")\
    .text('s3://cloudwatchlog-test-py/cwinput/').toDF('log')
    
digest_regex = '\[digestLog\(\d*\)\]'
content_df = log_df.filter(col('log').rlike(digest_regex))\
    .withColumn('content', from_json(split(col('log'), digest_regex, 2).getItem(1),merchant_gateway_schema))\
    .drop('log')
    
content_df.show(10,truncate=False)

#spark.createDataFrame(content_df.rdd, schema=merchant_gateway_schema)\
content_df.select(col("content.*"))\
    .repartition(10)\
    .write.mode('overwrite')\
    .option('header', 'false')\
    .parquet(
        path='s3://cloudwatchlog-test-py/cwoutput/'
    )
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
