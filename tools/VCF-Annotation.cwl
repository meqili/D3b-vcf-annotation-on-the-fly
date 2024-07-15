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
  ramMin: 32000
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
  default: 10
  inputBinding:
    position: 3
    prefix: --spark_driver_mem
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  default: 3
  inputBinding:
    position: 3
    prefix: --spark_executor_instance
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  default: 15
  inputBinding:
    position: 3
    prefix: --spark_executor_mem
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  default: 5
  inputBinding:
    position: 3
    prefix: --spark_executor_core
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_core
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_maxResultSize
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000
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
- id: input_file
  type: File
  inputBinding:
    prefix: --input_file
    position: 3
    shellQuote: false
- id: output_basename
  type: string
  inputBinding:
    prefix: --output_basename
    position: 4
    shellQuote: false
    valueFrom: $(inputs.input_file.nameroot)

outputs:
- id: vcf_output
  type: File
  outputBinding:
    glob: '*.VWB_annotated.tsv.gz'
    outputEval: $(inheritMetadata(self, inputs.input_file))

baseCommand:
- python
arguments:
- position: 1
  valueFrom: |-
    VCF-Annotation.py
  shellQuote: false
