from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

f = sc.textFile("new_content.txt")
data_rdd = f.map(lambda line : [x for x in line.split('|')])
payment_df = data_rdd.toDF(['2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17'])
payment_df = payment_df.drop('_17').drop('_18').drop('9').drop('4').drop('3').drop('10').drop('11').drop('16')


payment_df = payment_df.dropDuplicates()

def change_date_format(date_str):
    d = date_str.split('.')[0]
    m = date_str.split('.')[1]
    y = date_str.split('.')[2]
    return y + '-' + m + '-' + d

udf_cdf = udf(change_date_format,StringType())
payment_df = payment_df.withColumn('13',udf_cdf(payment_df['13']))\
                        .withColumn('14',udf_cdf(payment_df['14']))

udf_check_late = udf(lambda x,y: x<y,StringType())
payment_df = payment_df.withColumn('late',udf_cdf(payment_df['13'],payment_df['14']))
print payment_df['late']