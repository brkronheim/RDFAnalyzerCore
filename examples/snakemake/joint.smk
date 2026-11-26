from rdfproduction.joint_fit import joint_paths


joint_outputs = {name: str(path) for name, path in joint_paths(output_root).items()}
card_options = {key: joint[key] for key in ("cards", "combine_cards_exe") if key in joint}
card_options["fit"] = {key: joint["fit"][key] for key in ("fitting_backend", "combine_exe") if key in joint["fit"]}

localrules: combine_joint_datacards, run_joint_fit

rule joint_fit:
    input:
        joint_targets(joint, output_root)

rule combine_joint_datacards:
    input:
        receipts=list(dict.fromkeys(source["receipt"] for source in joint["cards"].values())),
        files=joint["saved_files"]
    output:
        cards=directory(joint_outputs["datacards"]),
        receipt=joint_outputs["datacards.json"]
    params:
        python=python,
        core=workflow_root,
        options=json.dumps(card_options, sort_keys=True)
    log:
        str(Path(joint_outputs["logs"]) / "datacards.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_joint_fit --options {params.options:q} --destination {output.cards:q} "
        "--receipt {output.receipt:q} 2>&1 | tee {log:q}"

rule run_joint_fit:
    input:
        cards=joint_outputs["datacards"],
        receipt=joint_outputs["datacards.json"]
    output:
        results=directory(joint_outputs["fit"]),
        receipt=joint_outputs["fit.json"]
    params:
        python=python,
        core=workflow_root,
        options=json.dumps(joint["fit"], sort_keys=True)
    log:
        str(Path(joint_outputs["logs"]) / "fit.log")
    shell:
        "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
        "{params.python:q} -m production_downstream fit --input {input.cards:q} --source-receipt {input.receipt:q} "
        "--options {params.options:q} --destination {output.results:q} --receipt {output.receipt:q} 2>&1 | tee {log:q}"

if "fit_diagnostics" in joint:
    localrules: run_joint_fit_diagnostics

    rule run_joint_fit_diagnostics:
        input:
            results=joint_outputs["fit"],
            receipt=joint_outputs["fit.json"]
        output:
            products=directory(joint_outputs["fit_diagnostics"]),
            receipt=joint_outputs["fit_diagnostics.json"]
        params:
            python=python,
            core=workflow_root,
            options=json.dumps(joint["fit_diagnostics"], sort_keys=True)
        log:
            str(Path(joint_outputs["logs"]) / "fit_diagnostics.log")
        shell:
            "PYTHONPATH={params.core:q}\"${{PYTHONPATH:+:$PYTHONPATH}}\" "
            "{params.python:q} -m production_downstream fit_diagnostics --input {input.results:q} "
            "--source-receipt {input.receipt:q} --options {params.options:q} "
            "--destination {output.products:q} --receipt {output.receipt:q} 2>&1 | tee {log:q}"
