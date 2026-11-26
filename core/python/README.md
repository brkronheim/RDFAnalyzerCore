# RDFAnalyzerCore Python code

| Area | Start here |
| --- | --- |
| Writing and running analyses | [rdfanalysis.py](rdfanalysis.py), the public `rdf*` authoring modules and their `.pyi` stubs, and the [Python authoring guide](../../docs/PYTHON_AUTHORING.md) |
| Authoring bindings and native integration | [bindings/](bindings/) and [optional_extensions.py](optional_extensions.py) |
| Running an existing analysis campaign | [rdfproduction/](rdfproduction/) and the [Snakemake guide](../../examples/snakemake/README.md) |
| Statistical inference | [statistical_inference/](statistical_inference/), [create_datacards.py](create_datacards.py), and the [Combine integration guide](../../docs/COMBINE_INTEGRATION.md) |
| Shared inputs and outputs | [dataset_manifest.py](dataset_manifest.py), [output_schema.py](output_schema.py), [metadata_io.py](metadata_io.py), and [run_bundle.py](run_bundle.py) |
| Existing LAW and production-manager entry points | [law/](law/), [production_manager.py](production_manager.py), and [production_monitor.py](production_monitor.py) |

## Production package

Use `python -m rdfproduction --help` from an environment with `core/python`
on `PYTHONPATH`. [specs.py](rdfproduction/specs.py) validates sealed production
inputs offline. [controller.py](rdfproduction/controller.py) owns run
admission, runtime preflight, and execution. [campaign.py](rdfproduction/campaign.py)
combines runs across eras. [downstream.py](rdfproduction/downstream.py),
[stitching.py](rdfproduction/stitching.py), and [joint_fit.py](rdfproduction/joint_fit.py)
compose stages through verified artifacts.

The [jobs package](rdfproduction/jobs/) divides production by lifecycle.
[prepare.py](rdfproduction/jobs/prepare.py) writes the partition plan using
[job_config.py](rdfproduction/jobs/job_config.py) and
[staging.py](rdfproduction/jobs/staging.py). [branches.py](rdfproduction/jobs/branches.py)
runs or verifies shared-filesystem partitions. [submit.py](rdfproduction/jobs/submit.py)
and [monitor.py](rdfproduction/jobs/monitor.py) manage Condor execution.
[output_validation.py](rdfproduction/jobs/output_validation.py) checks stage-out,
and [provenance.py](rdfproduction/jobs/provenance.py) builds output manifests.
[output_index.py](rdfproduction/output_index.py) publishes the verified
partition inventory consumed by downstream rules.

The previous flat imports and CLI names, including `python -m rdf_production`,
forward to the package. Snakemake uses the shared services directly. LAW
tasks wrap the same operations in their scheduler-specific interfaces.
