#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
id: VCF-Annotation
label: VCF-Annotation
doc: |-
  Get a list of deleterious variants in interested genes from specified study cohort(s) in the Kids First program.
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/d3b-bixu/pyspark:3.1.2
- class: InitialWorkDirRequirement
  listing:
  - entryname: VCF-Annotation.py
    entry:
      $include: ../scripts/VCF-Annotation.py
- class: InlineJavascriptRequirement
  expressionLib:
  - |2-

    var setMetadata = function(file, metadata) {
        if (!('metadata' in file))
            file['metadata'] = metadata;
        else {
            for (var key in metadata) {
                file['metadata'][key] = metadata[key];
            }
        }
        return file
    };

    var inheritMetadata = function(o1, o2) {
        var commonMetadata = {};
        if (!Array.isArray(o2)) {
            o2 = [o2]
        }
        for (var i = 0; i < o2.length; i++) {
            var example = o2[i]['metadata'];
            for (var key in example) {
                if (i == 0)
                    commonMetadata[key] = example[key];
                else {
                    if (!(commonMetadata[key] == example[key])) {
                        delete commonMetadata[key]
                    }
                }
            }
        }
        if (!Array.isArray(o1)) {
            o1 = setMetadata(o1, commonMetadata)
        } else {
            for (var i = 0; i < o1.length; i++) {
                o1[i] = setMetadata(o1[i], commonMetadata)
            }
        }
        return o1;
    };

inputs:
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 10
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
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000
- id: clinvar
  doc: clinvar parquet file dir
  type: File
  sbg:suggestedValue:
    name: clinvar_stable.tar.gz
    class: File
    path: 664f6fac18e23e6215aeb9ad
- id: gnomad4
  doc: gnomad4 parquet file dir
  type: File
  sbg:suggestedValue:
    name: gnomad40_exome_annovar.tar.gz
    class: File
    path: 664e03dc18e23e6215ad61d8
- id: input_file
  type: File
  inputBinding:
    prefix: --input_file
    position: 3
    shellQuote: false
- id: output_basemame
  type: string
  inputBinding:
    prefix: --output_basemame
    position: 3
    shellQuote: false
    valueFrom: $(inputs.input_file.nameroot)

outputs:
- id: vcf_output
  type: File
  outputBinding:
    glob: '*.VWB_annotated.tsv.gz'
    outputEval: $(inheritMetadata(self, inputs.input_file))

baseCommand:
- tar
- -xvf
arguments:
- position: 1
  valueFrom: |-
    $(inputs.clinvar.path) && tar -xvf $(inputs.gnomad4.path)
  shellQuote: false
- position: 2
  valueFrom: |-
    && spark-submit --packages io.projectglow:glow-spark3_2.12:1.1.2 \
    --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec  \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.executor.memory=$(inputs.spark_executor_mem)G \
    --conf spark.executor.instances=$(inputs.spark_executor_instance) \
    --conf spark.executor.cores=$(inputs.spark_executor_core) \
    --conf spark.driver.maxResultSize=$(inputs.spark_driver_maxResultSize)G \
    --conf spark.sql.broadcastTimeout=$(inputs.sql_broadcastTimeout)  \
    --driver-memory $(inputs.spark_driver_mem)G  \
    VCF-Annotation.py --clinvar ./$(inputs.clinvar.nameroot.replace(".tar", ""))/ --gnomad4 ./$(inputs.gnomad4.nameroot.replace(".tar", ""))/ 
  shellQuote: false
