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
- class: ResourceRequirement
  coresMin: 16
  ramMin: $(inputs.spark_driver_mem * 1000)
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/qqlii44/pyspark:3.5.1
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
  inputBinding:
    position: 3
    prefix: --spark_driver_mem
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  inputBinding:
    position: 3
    prefix: --spark_executor_instance
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  inputBinding:
    position: 3
    prefix: --spark_executor_mem
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  inputBinding:
    position: 3
    prefix: --spark_executor_core
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  inputBinding:
    position: 3
    prefix: --spark_driver_core
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  inputBinding:
    position: 3
    prefix: --spark_driver_maxResultSize
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  inputBinding:
    position: 3
    prefix: --sql_broadcastTimeout
- id: clinvar
  type: boolean
  inputBinding:
    position: 3
    prefix: --clinvar
- id: gnomad
  type: boolean
  inputBinding:
    position: 3
    prefix: --gnomad
- id: input_vcf_file
  type: File
  inputBinding:
    prefix: --input_vcf_file
    position: 3
    shellQuote: false

outputs:
- id: VWB_tsv_output
  type: File
  outputBinding:
    glob: '*.VWB_annotated.tsv.gz'
    outputEval: $(inheritMetadata(self, inputs.input_vcf_file))

baseCommand:
- python
arguments:
- position: 1
  valueFrom: |-
    VCF-Annotation.py --output_basename $(inputs.input_vcf_file.nameroot)
  shellQuote: false
