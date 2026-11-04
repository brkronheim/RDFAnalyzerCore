"""Export a validated portable model through Rabbit's native TensorWriter API."""
from pathlib import Path
import json
import numpy as np
from .errors import FitError


def write_tensor(model, output, *, sparse=True, zero_shape_policy="error", likelihood="native"):
    import hist
    from rabbit.tensorwriter import TensorWriter
    if zero_shape_policy not in ("error", "native"):
        raise FitError("Rabbit zero-shape policy must be error or native")

    def histogram(record):
        h = hist.Hist(hist.axis.Variable(record["edges"], name="observable"), storage=hist.storage.Weight())
        h.view().value[...] = record["values"]
        h.view().variance[...] = record["variances"]
        return h

    writer = TensorWriter(sparse=sparse)
    zero_audit = dict(policy=zero_shape_policy, relative_floor=float(np.exp(writer.logkepsilon)),
                      n_zero_varied_bins=0, by_nuisance={}, summed_affected_nominal=0.0)
    if likelihood == "combine":
        zero_audit.update(policy="combine: preserve zero shifts", relative_floor=None)
    nominal = {}
    for channel in model["channels"]:
        name = channel["name"]
        data = histogram(channel["data"])
        writer.add_channel(data.axes, name)
        writer.add_data(data, name)
        for process in channel["processes"]:
            key = (name, process["name"])
            nominal[key] = np.asarray(process["values"])
            writer.add_process(histogram(process), process["name"], name, signal=process["signal"])
    for effect in model["effects"]:
        if likelihood == "combine":
            if effect["kind"] == "shape":
                norm = nominal[(effect["channel"], effect["process"])]
                count = 0
                for direction in ("up", "down"):
                    mask = (norm > 0) & (np.asarray(effect[direction]["values"]) == 0)
                    count += int(mask.sum())
                    zero_audit["summed_affected_nominal"] += float(norm[mask].sum())
                zero_audit["n_zero_varied_bins"] += count
                if count:
                    name = effect["name"]
                    zero_audit["by_nuisance"][name] = zero_audit["by_nuisance"].get(name, 0)+count
            # Yield kernels read the original templates, not these placeholder
            # modifiers. Register each original constrained nuisance once.
            writer.book_systematic(effect["name"], groups=[g for g, names in model["groups"].items()
                                                         if effect["name"] in names] or None)
            continue
        common = dict(name=effect["name"], process=effect["process"], channel=effect["channel"],
                      groups=[g for g, names in model["groups"].items() if effect["name"] in names] or None,
                      symmetrize=None)
        if effect["kind"] == "lnN":
            factors = effect["factors"]
            # Rabbit's paired convention is (up, down), nested per process.
            uncertainty = factors[0] if len(factors) == 1 else [(factors[1], factors[0])]
            writer.add_norm_systematic(uncertainty=uncertainty, **common)
        else:
            norm = nominal[(effect["channel"], effect["process"])]
            up, down = np.asarray(effect["up"]["values"]), np.asarray(effect["down"]["values"])
            if np.any((norm == 0) & ((up != 0) | (down != 0))):
                raise FitError(f"variation creates support from zero nominal: {effect['name']}; prepare a common support policy first")
            count = 0
            for shifted in (up, down):
                mask = (norm > 0) & (shifted <= 0)
                count += int(mask.sum())
                zero_audit["summed_affected_nominal"] += float(norm[mask].sum())
            if count:
                if zero_shape_policy == "error":
                    raise FitError(f"log-normal shape has zero varied yield: {effect['name']}; "
                                   "prepare a common support policy or explicitly select --rabbit-zero-shape-policy native")
                zero_audit["n_zero_varied_bins"] += count
                name = effect["name"]
                zero_audit["by_nuisance"][name] = zero_audit["by_nuisance"].get(name, 0) + count
            writer.add_systematic([histogram(effect["up"]), histogram(effect["down"])],
                                  kfactor=effect["factor"], mirror=False, **common)
    output = Path(output)
    writer.write(str(output.parent), output.name, meta_data_dict={
        "rdf_model_identity": model["identity"], "sources": model["sources"],
        "likelihood": likelihood,
        "shape_interpolation": ("Combine v11 shape/hist-mode 1" if likelihood == "combine" else
                                "Rabbit asymmetric log-normal; not asserted equivalent to Combine shape interpolation"),
        "zero_shape_audit": zero_audit,
    })
    output.with_suffix(".metadata.json").write_text(json.dumps(zero_audit, indent=2) + "\n")
    return output
