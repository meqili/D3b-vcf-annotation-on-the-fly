import argparse
from argparse import RawTextHelpFormatter
from pyspark.sql import SparkSession, functions as F
import glow

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G yours.py',
    formatter_class=RawTextHelpFormatter)

parser.add_argument('--output_basename', required=True, help='Output basename for the annotated file')
parser.add_argument('--input_file', required=True, help='Input file path for AR_comphet file')
parser.add_argument('--clinvar', action='store_true', help='Include ClinVar data')
parser.add_argument('--gnomad', action='store_true', help='Include GnomAD data')
parser.add_argument('--spark_executor_mem', type=int, default=4, help='Spark executor memory in GB')
parser.add_argument('--spark_executor_instance', type=int, default=1, help='Number of Spark executor instances')
parser.add_argument('--spark_executor_core', type=int, default=1, help='Number of Spark executor cores')
parser.add_argument('--spark_driver_maxResultSize', type=int, default=1, help='Spark driver max result size in GB')
parser.add_argument('--sql_broadcastTimeout', type=int, default=300, help='Spark SQL broadcast timeout in seconds')
parser.add_argument('--spark_driver_core', type=int, default=1, help='Number of Spark driver cores')
parser.add_argument('--spark_driver_mem', type=int, default=4, help='Spark driver memory in GB')

args = parser.parse_args()

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName('glow_pyspark') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes', 'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()

# Register Glow with Spark
spark = glow.register(spark)

# --------------------------------------
# Part 1: reading VCF file
# --------------------------------------
cond = ['chromosome', 'start', 'reference', 'alternate']
vcf_df = spark \
    .read.format('vcf') \
    .load(args.input_file) \
    .withColumn('chromosome', F.regexp_replace(F.col('contigName'), r'(?i)^chr[^\dXxYyMm]*', '')) \
    .withColumn('alternate', F.when(F.size(F.col('alternateAlleles')) == 1, F.col('alternateAlleles').getItem(0)).otherwise(F.lit(None))) \
    .withColumnRenamed('referenceAllele', 'reference') \
    .drop('contigName', 'referenceAllele', 'alternateAlleles')
vcf_df = vcf_df \
    .select(cond + ['end'])

if args.clinvar:
    clv = spark.read.format("delta") \
        .load("s3a://kf-strides-public-vwb-prd/clinvar") \
        .drop('end', 'filters')
    vcf_df = vcf_df \
        .join(clv, cond, 'left')

if args.gnomad:
    gnomad_exomes_v2_1_1 = spark.read.format("delta") \
        .load("s3a://kf-strides-public-vwb-prd/gnomad_exomes_v2_1_1_liftover_grch38")
    vcf_df = vcf_df \
        .join(gnomad_exomes_v2_1_1, cond, 'left')

output_file = args.output_basename + '.VWB_annotated.tsv.gz'
vcf_df.toPandas() \
    .to_csv(output_file, sep='\t', header=True, index=False, lineterminator='\n', compression='gzip')