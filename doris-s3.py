import sys
import datetime


from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




def get_fixed_previous_date(fixed_num):
    return (datetime.datetime.now() - datetime.timedelta(days=fixed_num)).strftime('%Y-%m-%d')




sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


args = getResolvedOptions(sys.argv, ['JOB_NAME'])


previous_date = get_fixed_previous_date(1)


df1 = glueContext.create_dynamic_frame_from_catalog(
    database='payermax_analysis',
    table_name='dwd_dwd_receipt_trade_channel_all_1di',
    transformation_ctx='df1',
    additional_options={
        'sampleQuery': '''
            select 
                * 
            from
                dwd.dwd_receipt_trade_channel_all_1di
            where 
                utc_trade_create_time >= date_add(date_trunc('day', '{previous_date}'), interval -31 day)
            and 
                utc_trade_create_time < date_add(date_trunc('day', '{previous_date}'), interval 1 day)
            and 
                utc_trade_modified_time >= date_trunc('day', '{previous_date}')
            and 
                utc_trade_modified_time < date_add(date_trunc('day', '{previous_date}'), interval 1 day)
        '''.format(previous_date=previous_date)
    }
).toDF()


df2 = glueContext.create_dynamic_frame_from_catalog(
    database="payermax_analysis",
    table_name="ods_ods_pay_member_ma_merchant",
    transformation_ctx="df2"
).toDF()


df3 = glueContext.create_dynamic_frame_from_catalog(
    database="payermax_analysis",
    table_name="ods_ods_fin_merchant_merchant_base",
    transformation_ctx="df3"
).toDF()


df1.createOrReplaceTempView('dwd_dwd_receipt_trade_channel_all_1di')
df2.createOrReplaceTempView('ods_ods_pay_member_ma_merchant')
df3.createOrReplaceTempView('ods_ods_fin_merchant_merchant_base')


sql = '''
    select 
        t1.*,
        t2.merchant_name,
        t2.inner_name,
        t2.bd_contacter
    from 
        dwd_dwd_receipt_trade_channel_all_1di t1
    left join (
        select 
            merchant_no,
            merchant_name,
            inner_name,
            bd_contacter
        from 
            ods_ods_pay_member_ma_merchant 
        union all
        select 
            merchant_no,
            merchant_name,
            inner_name,
            bd_contacter
        from 
            ods_ods_fin_merchant_merchant_base  
    ) t2 
    on
        t1.merchant_no = t2.merchant_no;
'''.format(previous_date=previous_date)


sql_df = spark.sql(sql)
sql_df.repartition(10).write.mode('overwrite').parquet(
    path='s3://pay-data-sg1/payermax_analysis/ods/ods_receipt_trade_channel/dt=' + previous_date + '/'
)


job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
