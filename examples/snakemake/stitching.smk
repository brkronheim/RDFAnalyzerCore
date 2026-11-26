source_campaign = stitching["source"]
source_plan = stitching["source_plan"]
source_specs = source_campaign["era_specs"]
source_ready = str(source_campaign["preflight_path"])
source_root = source_campaign["output_root"]
source_index = str(source_root / "{era}" / "output_index.json")
source_prepared = str(counter_artifacts["prepared"])
source_smoke = str(counter_artifacts["smoke"])
source_plan_path = str(source_campaign["plan_path"])
source_generated = [str(path) for path in source_campaign["generated"].values()]
validation_options = [] if stitching["require_stitched_histograms"] else ["--no-stitched-histograms"]

localrules: plan_stitch_source, preflight_stitch_source, prepare_stitch_source, smoke_stitch_source, execute_stitch_source, derive_stitch_weights, bind_stitch_application, validate_stitch_application

rule stitch_counters:
    input:
        expand(source_index, era=era_specs)

rule stitch_weights:
    input:
        expand(weights, era=era_specs)

rule stitch:
    input:
        expand(validation_report, era=era_specs)

rule plan_stitch_source:
    input:
        [str(path) for path in source_campaign["inputs"]]
    output:
        source_plan_path, source_generated
    params:
        plan_id=source_plan["plan_id"]
    run:
        publish_plan(source_campaign)

rule preflight_stitch_source:
    input:
        plan=source_plan_path,
        specs=[str(path) for path in source_specs.values()]
    output:
        source_ready
    params:
        python=python
    log:
        str(source_root / "logs" / "preflight.log")
    run:
        preflight_campaign(source_campaign, python=params.python, log=log[0])

rule prepare_stitch_source:
    input:
        spec=lambda wildcards: str(source_specs[wildcards.era]),
        ready=source_ready
    output:
        source_prepared
    params:
        python=python,
        core=workflow_root
    log:
        str(source_root / "{era}" / "logs" / "prepare.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_campaign prepare {input.spec:q} --receipt {output:q} "
        "2>&1 | tee {log:q}"

rule smoke_stitch_source:
    input:
        spec=lambda wildcards: str(source_specs[wildcards.era]),
        prepared=source_prepared
    output:
        source_smoke
    params:
        python=python,
        core=workflow_root
    log:
        str(source_root / "{era}" / "logs" / "smoke.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_campaign smoke {input.spec:q} --receipt {output:q} "
        "2>&1 | tee {log:q}"

rule execute_stitch_source:
    input:
        spec=lambda wildcards: str(source_specs[wildcards.era]),
        ready=source_ready
    output:
        source_index
    params:
        python=python,
        core=workflow_root,
        completion=lambda wildcards: condor_completion if source_plan["eras"][wildcards.era]["inspection"].get("backend", "htcondor") == "htcondor" else []
    threads:
        lambda wildcards: execution_threads(source_plan, wildcards.era)
    resources:
        condor_runs=lambda wildcards: int(source_plan["eras"][wildcards.era]["inspection"].get("backend", "htcondor") == "htcondor")
    log:
        str(source_root / "{era}" / "logs" / "production.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m rdf_production execute {input.spec:q} --output-index {output:q} "
        "--local-workers {threads} {params.completion:q} 2>&1 | tee {log:q}"

rule derive_stitch_weights:
    input:
        spec=lambda wildcards: str(source_specs[wildcards.era]),
        config=lambda wildcards: str(stitching["configs"][wildcards.era]),
        index=source_index
    output:
        weights=weights,
        cross_sections=cross_sections
    params:
        python=python,
        core=workflow_root,
        destination=lambda wildcards: str(output_root / wildcards.era / "stitch")
    log:
        str(output_root / "{era}" / "logs" / "stitch-derive.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_stitching derive {input.spec:q} {input.config:q} "
        "{input.index:q} {params.destination:q} 2>&1 | tee {log:q}"

rule bind_stitch_application:
    input:
        spec=lambda wildcards: str(era_specs[wildcards.era]),
        weights=weights,
        counts=source_index
    output:
        application_spec
    params:
        python=python,
        core=workflow_root
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_stitching bind {input.spec:q} {input.weights:q} {output:q} "
        "--counter-index {input.counts:q}"

rule validate_stitch_application:
    input:
        spec=application_spec,
        config=lambda wildcards: str(stitching["configs"][wildcards.era]),
        index=output_index,
        weights=weights,
        cross_sections=cross_sections
    output:
        validation_report
    params:
        python=python,
        core=workflow_root,
        destination=lambda wildcards: str(output_root / wildcards.era / "stitch"),
        options=validation_options
    log:
        str(output_root / "{era}" / "logs" / "stitch-validate.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_stitching validate {input.spec:q} {input.config:q} "
        "{input.index:q} {params.destination:q} --weights {input.weights:q} "
        "--cross-sections {input.cross_sections:q} {params.options:q} 2>&1 | tee {log:q}"
