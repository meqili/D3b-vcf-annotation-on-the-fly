#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: Workflow
id: vcf-annotation-on-the-fly
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: InlineJavascriptRequirement
- class: StepInputExpressionRequirement

inputs:
- id: input_vcf_file
  type: File
- id: clinvar
  type: boolean
- id: gnomad
  type: boolean
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 48
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  default: 3
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  default: 34
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  default: 5
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  default: 2
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000

outputs:
- id: VWB_vcf_output
  type: File
  outputSource:
  - Attach_VWB/VWB_vcf_output

steps:
- id: VCF-Annotation
  in:
  - id: spark_driver_mem
    source: spark_driver_mem
  - id: spark_executor_instance
    source: spark_executor_instance
  - id: spark_executor_mem
    source: spark_executor_mem
  - id: spark_executor_core
    source: spark_executor_core
  - id: spark_driver_core
    source: spark_driver_core
  - id: spark_driver_maxResultSize
    source: spark_driver_maxResultSize
  - id: sql_broadcastTimeout
    source: sql_broadcastTimeout
  - id: clinvar
    source: clinvar
  - id: gnomad
    source: gnomad
  - id: input_vcf_file
    source: input_vcf_file
  run: tools/VCF-Annotation.cwl
  out:
  - id: VWB_tsv_output
- id: Attach_VWB
  run: tools/Attach_VWB.cwl
  in: 
  - id: input_vcf_file
    source: input_vcf_file
  - id: input_tsv_file
    source: VCF-Annotation/VWB_tsv_output
  out:
  - id: VWB_vcf_output