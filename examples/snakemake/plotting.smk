from native_config_io import load_yaml_document
from run_collection import isolated_name


plotting = config["plotting"]
if (not isinstance(plotting, dict) or plotting.keys() - {"configs", "inputs", "executable", "workers"}
        or not isinstance(plotting.get("configs"), dict)):
    raise ValueError("plotting requires an executable and era-to-config mapping")

plot_configs = {str(era): path for era, path in plotting["configs"].items()}
if (not plot_configs or len(plot_configs) != len(plotting["configs"])
        or (campaign_plotting and set(plot_configs) - era_specs.keys())
        or any(not isinstance(path, str) or not path for path in plot_configs.values())):
    raise ValueError("plotting.configs must identify selected eras and nonempty paths")
raw_inputs = plotting.get("inputs", {})
if not isinstance(raw_inputs, dict):
    raise ValueError("plotting.inputs must map eras to lists of extra local histogram files")
plot_inputs = {str(era): files for era, files in raw_inputs.items()}
if (len(plot_inputs) != len(raw_inputs) or plot_inputs.keys() - plot_configs.keys()
        or any(not isinstance(files, list) or any(not isinstance(f, str) or not f for f in files)
               for files in plot_inputs.values())):
    raise ValueError("plotting.inputs requires lists of files for configured eras")
plot_executable = plotting.get("executable")
if not isinstance(plot_executable, str) or not plot_executable:
    raise ValueError("plotting.executable must identify the rdf-plot executable")
plot_workers = plotting.get("workers", 1)
if type(plot_workers) is not int or plot_workers < 1:
    raise ValueError("plotting.workers must be a positive integer")


plot_products = []
for era, plot_config in plot_configs.items():
    isolated_name("plots", era)
    document = load_yaml_document(plot_config, typed=True)
    requests = document.get("plots") if isinstance(document, dict) else document
    if not isinstance(requests, list) or not requests:
        raise ValueError(f"{plot_config}: expected a nonempty sequence of plot requests")
    histograms = list(plot_inputs.get(era, []))
    images = []
    for request in requests:
        if not isinstance(request, dict):
            raise ValueError(f"{plot_config}: each plot request must be a mapping")
        for process in request.get("processes", []):
            if not isinstance(process, dict):
                raise ValueError(f"{plot_config}: plot processes must be mappings")
            sources = process.get("files") or ([request["metaFile"]] if request.get("metaFile") else [])
            if not isinstance(sources, list) or any(not isinstance(path, str) for path in sources):
                raise ValueError(f"{plot_config}: process files must be a sequence of file paths")
            histograms.extend(path for path in sources if "://" not in path)
        extra_outputs = request.get("additionalOutputs", [])
        if not isinstance(extra_outputs, list):
            raise ValueError(f"{plot_config}: additionalOutputs must be a sequence of output paths")
        requested = [request.get("outputFile"), *extra_outputs]
        if any(not isinstance(path, str) or not path for path in requested):
            raise ValueError(f"{plot_config}: each plot requires an outputFile and valid additionalOutputs")
        images.extend(str(Path(path).resolve()) for path in requested)
    # A campaign can merge the same histogram sources as side effects of a
    # verified merge. In that mode the receipt is the producer's declared edge.
    if campaign_plotting and era not in plot_inputs:
        histograms = [merge_receipt.format(era=era)]
    report_path = plot_report.format(era=era)
    products = [report_path, *images]
    if len(set(products)) != len(products) or set(products).intersection(plot_products):
        raise ValueError(f"{plot_config}: plot destinations must be unique across requests and eras")
    plot_products.extend(products)

    rule:
        name: f"plot_era_{era}"
        localrule: True
        input:
            histograms=list(dict.fromkeys(histograms)),
            config=plot_config,
            executable=plot_executable
        output:
            report=ensure(report_path, non_empty=True),
            images=[ensure(path, non_empty=True) for path in images]
        threads:
            plot_workers
        log:
            str(output_root / era / "logs" / "plots.log")
        shell:
            "{input.executable:q} --config {input.config:q} --workers {threads} --report {output.report:q} "
            "2>&1 | tee {log:q}"


rule plots:
    input:
        plot_products
