import os, sys, threading
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession, DataFrame

conf = SparkConf().setAppName('partitioning')
sc = SparkContext(conf=conf)
spark = (SparkSession.builder.config(conf=conf)
         # .config("hive.exec.dynamic.partition", "true") \
         # .config("hive.exec.dynamic.partition.mode", "nonstrict")
         .enableHiveSupport()
         .getOrCreate())
sql_context = SQLContext(sc)


def install_and_import(packages):
    for package_name in packages:
        try:
            __import__(package_name)
        except ImportError:
            import os
            os.system('pip-2.7 install --user --upgrade ' + package_name)
            # sc.install_pypi_package(package_name)
            __import__(package_name)


packages = ['boto3', 'pandas', 'numpy']
install_and_import(packages)

print(sys.version_info)

s3 = boto3.resource('s3')
bucket_name = 'gbsc-aws-project-annohive-dev-user-krferrit-us-west-1'
prefix = '1000Orig-half2-parquet-partitioned'

print('Loading data')
df = spark.read.load('s3n://gbsc-aws-project-annohive-dev-user-krferrit-us-west-1' \
                     + '/1000Orig-half2-parquet' \
                     + '/*')
print('Loaded %d rows from s3' % df.count())

print('Converting POS to int')
df = df.withColumn('POS', df['POS'].cast('int'))
df = df.orderBy('POS')
print(df)

print('Bucketing and writing dataframe')


def get_max_pos(df):
    return int(df.selectExpr('max(POS) as m').collect()[0].asDict()['m'])


def partition_and_submit_reference(global_df, reference_name, pos_bin_count=4000):
    print('partition_and_submit_reference, reference_name=' + reference_name)
    filtered_df = global_df.filter(global_df['reference_name'] == str(reference_name))
    print('partition reference_name=%s count=%d' % (
        reference_name,
        filtered_df.count()
    ))
    # add binning column
    # get max pos
    global_max_pos = get_max_pos(global_df)
    bin_count = int(float(global_max_pos) / pos_bin_count)
    print('global_max_pos=%d, bin_count=%d' % (global_max_pos, bin_count))
    filtered_df = filtered_df.withColumn('POS_BIN_ID', (filtered_df.POS / bin_count).cast('int'))
    #    output_path = 's3a://{bucket}/{prefix}/POS_BIN_ID={binID}/' % ()
    filtered_df.repartition('POS_BIN_ID', 'reference_name') \
        .write.mode('overwrite') \
        .partitionBy('POS_BIN_ID', 'reference_name') \
        .parquet('s3a://' + bucket_name + '/1000Orig-half2-bucketed-4000/')
    print('finished writing parquet')


# print('Writing partitions to S3')
# (df
#     .write
#     #.option('path', 's3a://'+bucket_name+'/1000Orig-half2-bucketed-4000/') 
#     .format('hive') 
#     .mode('overwrite')
#     #.bucketBy(4000, 'POS')
#     #.foreachPartition(partition_handler)
#     .parquet('s3a://'+bucket_name+'/1000Orig-half2-bucketed-4000/')
#     #.saveAsTable('1000Orig_half2_bucketed_4000')
# )

references = [str(c) for c in range(1, 23)] + ['X', 'Y']
# references = ['1']


def print_counts(global_df):
    print('global_df count=%d' % global_df.count())

    for reference_name in references:
        filtered_df = global_df.filter('REF = "%s"' % str(reference_name))
        print('REF=%s, count=%d' % (reference_name, filtered_df.count()))
# For simple test of connectivity
# print_counts(df)


# t_list = []
for ref_name in references:
    partition_and_submit_reference(df, ref_name, 4000)
    # Turned off threading for now, just do one reference at a time
    # t = threading.Thread(
    #        target=partition_and_submit_reference, 
    #        args=(df, ref_name, 4000))
    # t.start()
    # t_list.append(t)

# for t in t_list:
#    t.join()
