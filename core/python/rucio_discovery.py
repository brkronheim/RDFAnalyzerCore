"""Compatibility entry point for generic sample parsing and optional CMS Rucio."""
import sys
if __package__:
    from .optional_extensions import load_extension_module
    from .sample_config import SampleConfigData, load_legacy_sample_config, load_sample_config
else:
    from optional_extensions import load_extension_module
    from sample_config import SampleConfigData, load_legacy_sample_config, load_sample_config

_provider = load_extension_module("cms", "rucio_discovery")
if _provider is not None:
    sys.modules[__name__] = _provider
else:
    def __getattr__(name):
        if name.startswith("__"):
            raise AttributeError(name)
        provider = load_extension_module("cms", "rucio_discovery", required=True)
        return getattr(provider, name)
