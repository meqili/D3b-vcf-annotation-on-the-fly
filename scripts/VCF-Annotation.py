import argparse
from argparse import RawTextHelpFormatter
import pyspark
import pyspark.sql.functions as F
import glow

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G yours.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('--output_basemame')
parser.add_argument('--input_file', help='Input file path for AR_comphet file')
parser.add_argument('--clinvar', help='clinvar parquet file dir')
parser.add_argument('--gnomad4', help='gnomad4 exome annovar parquet file dir')
args = parser.parse_args()

# Create spark session
spark = (
    pyspark.sql.SparkSession.builder.appName("PythonPi")
    .getOrCreate()
    )
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
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

clv = spark \
    .read \
    .parquet(args.clinvar) \
    .drop('end', 'filters')
gnomad4 = spark \
    .read \
    .parquet(args.gnomad4) \
    .drop('end', 'Ref', 'Alt')

vcf_output = vcf_df \
    .join(clv, cond, 'left') \
    .join(gnomad4, cond, 'left')

output_file = args.output_basemame + '.VWB_annotated.tsv.gz'
vcf_output.toPandas() \
    .to_csv(output_file, sep='\t', header=True, index=False, line_terminator='\n', compression='gzip')