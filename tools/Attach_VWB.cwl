#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
id: Attach_VWB
label: Attach_VWB
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: DockerRequirement
  dockerPull: 'perl:5.38.2'
- class: InitialWorkDirRequirement
  listing:
  - entryname: Attach_VWB.pl
    entry:
      $include: ../scripts/Attach_VWB.pl
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
- id: input_vcf_file
  type: File
- id: input_tsv_file
  type: File

outputs:
- id: VWB_vcf_output
  type: File
  outputBinding:
    glob: '*.VWB.vcf.gz'
    outputEval: $(inheritMetadata(self, inputs.input_vcf_file))

baseCommand: [/bin/bash, -c]
arguments:
- position: 1
  valueFrom: |-
    set -eo pipefail; gunzip -c $(inputs.input_vcf_file.path) > temp.vcf; gunzip -c $(inputs.input_tsv_file.path) > temp.tsv; perl Attach_VWB.pl temp.vcf temp.tsv | gzip -c > $(inputs.input_vcf_file.basename.replace("vcf.gz", "VWB.vcf.gz"))
  shellQuote: false
