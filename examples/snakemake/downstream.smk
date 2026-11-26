from rdfproduction.downstream import (
    CONSUMER_STAGES, card_shapes, raw_card_inputs, downstream_paths, downstream_targets, needs_manifest_merge, saved_manifest_files,
)
from run_bundle import load_artifact_receipt


paths = {key: str(path) for key, path in downstream_paths(output_root, "{era}").items()}
consumer_eras = {stage: [era for era, settings in downstream.items() if stage in settings]
                 for stage in CONSUMER_STAGES}


def manifest_merge_sources(wildcards):
    return downstream[wildcards.era].get("job_manifests", [paths["input_manifests"].format(era=wildcards.era)])


def downstream_upstream_stage(wildcards):
    if wildcards.stage == "fit_diagnostics":
        return downstream[wildcards.era]["fit_diagnostics"]["source"]
    if wildcards.stage == "fit" and "input_card" not in downstream[wildcards.era]:
        return "datacards"
    return None


def downstream_saved_source(wildcards):
    settings = downstream[wildcards.era]
    if wildcards.stage == "fit":
        return settings.get("saved_datacards")
    if wildcards.stage == "fit_diagnostics":
        saved = settings.get("saved_fit")
        if saved and saved["kind"] == downstream_upstream_stage(wildcards):
            return saved
    return None


def downstream_consumer_input(wildcards):
    era = wildcards.era
    settings = downstream[era]
    if wildcards.stage == "fit" and "input_card" in settings:
        return settings["input_card"]
    if wildcards.stage == "datacards" and settings["datacards"].get("raw_inputs"):
        return settings["datacards"]["config"]
    saved = downstream_saved_source(wildcards)
    if saved:
        return saved["directory"]
    upstream = downstream_upstream_stage(wildcards)
    if upstream:
        return paths[upstream].format(era=era)
    return downstream[era].get("manifest", paths["manifest_merge"].format(era=era))


def downstream_consumer_settings(wildcards):
    stage = wildcards.stage
    setting = downstream[wildcards.era][stage]
    return {"config": setting} if stage == "plots" else setting


def downstream_source_receipt(wildcards):
    saved = downstream_saved_source(wildcards)
    if saved:
        return [saved["receipt"]]
    upstream = downstream_upstream_stage(wildcards)
    if upstream:
        return [paths[f"{upstream}_receipt"].format(era=wildcards.era)]
    if wildcards.stage == "fit" or (wildcards.stage == "datacards" and
                                     downstream[wildcards.era]["datacards"].get("raw_inputs")):
        return []
    if "manifest" not in downstream[wildcards.era]:
        return [paths["manifest_merge_receipt"].format(era=wildcards.era)]
    return []


def downstream_source_files(wildcards):
    settings = downstream[wildcards.era]
    if wildcards.stage == "fit" and "input_card" in settings:
        return card_shapes(settings["input_card"])
    if wildcards.stage == "datacards" and settings["datacards"].get("raw_inputs"):
        return raw_card_inputs(settings["datacards"]["config"])
    saved = downstream_saved_source(wildcards)
    if saved:
        return list(load_artifact_receipt(saved["receipt"])["files"])
    if not downstream_upstream_stage(wildcards) and "manifest" in downstream[wildcards.era]:
        return saved_manifest_files([downstream[wildcards.era]["manifest"]], roles=("histograms",))
    return []


def downstream_config_files(wildcards):
    options = downstream_consumer_settings(wildcards)
    return [options[key] for key in ("config", "dataset_manifest") if key in options]


localrules: downstream_manifest_merge, downstream_consume

rule downstream:
    input:
        downstream_targets(downstream, output_root),
        joint_targets(joint, output_root) if joint else []

rule refill:
    input:
        expand(paths["refill_index"], era=refill_plans)

rule manifest_merge:
    input:
        expand(paths["manifest_merge_receipt"], era=[era for era, cfg in downstream.items() if needs_manifest_merge(cfg)])

rule manifest_plots:
    input:
        expand(paths["plots_receipt"], era=consumer_eras["plots"])

rule datacards:
    input:
        expand(paths["datacards_receipt"], era=consumer_eras["datacards"])

rule fits:
    input:
        expand(paths["fit_receipt"], era=consumer_eras["fit"])

rule analytic_fits:
    input:
        expand(paths["analytic_fit_receipt"], era=consumer_eras["analytic_fit"])

rule fit_diagnostics:
    input:
        expand(paths["fit_diagnostics_receipt"], era=consumer_eras["fit_diagnostics"])

if downstream_uses_production:
    localrules: downstream_bind_refill, downstream_execute_refill, downstream_publish_manifests

    rule downstream_bind_refill:
        input:
            spec=lambda wildcards: execution_specs[wildcards.era],
            index=output_index,
            template=lambda wildcards: downstream[wildcards.era]["refill"]
        output:
            directory(paths["refill_binding"])
        params:
            python=python,
            core=workflow_root
        log:
            str(output_root / "{era}" / "logs" / "refill-bind.log")
        shell:
            "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
            "{params.python:q} -m production_downstream refill --spec {input.spec:q} --index {input.index:q} "
            "--template {input.template:q} --destination {output:q} 2>&1 | tee {log:q}"

    rule downstream_execute_refill:
        input:
            binding=paths["refill_binding"]
        output:
            paths["refill_index"]
        params:
            python=python,
            core=workflow_root,
            spec=lambda wildcards: str(Path(paths["refill_binding"].format(era=wildcards.era)) / "spec.json")
        threads:
            lambda wildcards: execution_slots(refill_plans[wildcards.era])
        resources:
            condor_runs=lambda wildcards: int(refill_plans[wildcards.era].get("backend", "htcondor") == "htcondor")
        log:
            str(output_root / "{era}" / "logs" / "refill.log")
        shell:
            "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
            "{params.python:q} -m rdf_production execute {params.spec:q} --output-index {output:q} "
            "--local-workers {threads} 2>&1 | tee {log:q}"

    rule downstream_publish_manifests:
        input:
            spec=lambda wildcards: execution_specs[wildcards.era],
            index=output_index,
            refill=lambda wildcards: [paths[key].format(era=wildcards.era)
                                      for key in ("refill_binding", "refill_index")]
                                     if wildcards.era in refill_plans else []
        output:
            directory(paths["input_manifests"])
        params:
            python=python,
            core=workflow_root,
            refill=lambda wildcards, input: ["--refill", str(Path(input.refill[0]) / "spec.json"), input.refill[1]]
                                             if input.refill else []
        log:
            str(output_root / "{era}" / "logs" / "publish-manifests.log")
        shell:
            "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
            "{params.python:q} -m production_downstream manifests --spec {input.spec:q} --index {input.index:q} "
            "{params.refill:q} --destination {output:q} 2>&1 | tee {log:q}"

rule downstream_manifest_merge:
    input:
        manifests=manifest_merge_sources,
        files=lambda wildcards: saved_manifest_files(downstream[wildcards.era].get("job_manifests", []))
    output:
        products=directory(paths["manifest_merge"]),
        receipt=paths["manifest_merge_receipt"]
    params:
        python=python,
        core=workflow_root,
        input_flag=lambda wildcards: "--supplied" if "job_manifests" in downstream[wildcards.era] else "--production"
    log:
        str(output_root / "{era}" / "logs" / "manifest-merge.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_downstream merge {params.input_flag:q} {input.manifests:q} "
        "--destination {output.products:q} --receipt {output.receipt:q} 2>&1 | tee {log:q}"

rule downstream_consume:
    input:
        source=downstream_consumer_input,
        receipt=downstream_source_receipt,
        files=downstream_source_files,
        config=downstream_config_files
    output:
        products=directory(str(output_root / "{era}" / "downstream" / "{stage}")),
        receipt=str(output_root / "{era}" / "downstream" / "{stage}.json")
    wildcard_constraints:
        stage="|".join(CONSUMER_STAGES)
    params:
        python=python,
        core=workflow_root,
        options=lambda wildcards: json.dumps(downstream_consumer_settings(wildcards), sort_keys=True),
        receipt=lambda wildcards, input: ["--source-receipt", input.receipt[0]] if input.receipt else []
    log:
        str(output_root / "{era}" / "logs" / "downstream-{stage}.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_downstream {wildcards.stage:q} --input {input.source:q} "
        "--options {params.options:q} {params.receipt:q} "
        "--destination {output.products:q} --receipt {output.receipt:q} 2>&1 | tee {log:q}"
