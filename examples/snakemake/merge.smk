metadata_merger = config.get("metadata_merger")
merger_options = ["--metadata-merger", metadata_merger] if metadata_merger else []
nominal_only = config.get("nominal_only", False)
if type(nominal_only) is not bool:
    raise ValueError("nominal_only must be a boolean")
merge_mode = "--nominal-only" if nominal_only else "--sparse-only"
compatibility = config.get("semantic_config_compatibility")
if compatibility is not None and (not isinstance(compatibility, str) or not compatibility.strip()):
    raise ValueError("semantic_config_compatibility must be a path")
compatibility_options = ["--semantic-config-compatibility", compatibility] if compatibility else []

localrules: merge_era

rule merge:
    input:
        expand(merge_receipt, era=merge_eras)

rule merge_era:
    input:
        index=merge_index,
        merger=[metadata_merger] if metadata_merger else [],
        compatibility=[compatibility] if compatibility else []
    output:
        merge_receipt
    params:
        python=python,
        core=workflow_root,
        destination=lambda wildcards: str(output_root / wildcards.era / "merged"),
        options=[merge_mode, *merger_options, *compatibility_options],
        hadd_jobs=merge_resources["hadd_jobs"],
        xrdcp_jobs=merge_resources["xrdcp_jobs"]
    threads:
        merge_resources["data_merge_threads"]
    log:
        str(output_root / "{era}" / "logs" / "merge.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m rdf_production merge --output-index {input.index:q} "
        "--destination {params.destination:q} --receipt {output:q} "
        "--data-merge-threads {threads} --hadd-jobs {params.hadd_jobs} --xrdcp-jobs {params.xrdcp_jobs} "
        "{params.options:q} 2>&1 | tee {log:q}"
