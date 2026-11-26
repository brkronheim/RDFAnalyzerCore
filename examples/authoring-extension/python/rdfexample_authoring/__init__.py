"""An independently importable authoring provider; metadata needs only stdlib."""
from .modules import make_ht_module, module_specs, sum_above_threshold

__all__=['make_ht_module','module_specs','sum_above_threshold']
