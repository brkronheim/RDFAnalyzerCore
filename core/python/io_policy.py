"""Transport policy supplied by an optional experiment extension.

Submission scripts embed the resolved values, so workers do not need the
extension checkout or Python package for URL routing and stage-in.
"""
if __package__:
    from .optional_extensions import load_extension_service
else:
    from optional_extensions import load_extension_service

_provider = load_extension_service("io_policy")
SITE_REDIRECTOR_MARKER = getattr(_provider, "SITE_REDIRECTOR_MARKER", "/__rdf_no_site_prefix__/")
GENERIC_REDIRECTOR_HOSTS = getattr(_provider, "GENERIC_REDIRECTOR_HOSTS", set())
DEFAULT_REDIRECTORS = getattr(_provider, "DEFAULT_REDIRECTORS", [])
GLOBAL_REDIRECTOR = getattr(_provider, "GLOBAL_REDIRECTOR", "")
PUBLIC_STORAGE_PREFIXES = getattr(_provider, "PUBLIC_STORAGE_PREFIXES", ())
SITE_ENV_VARS = getattr(_provider, "SITE_ENV_VARS", ("SITE_NAME",))
SITE_DOMAINS = getattr(_provider, "SITE_DOMAINS", {})
HOST_SITES = getattr(_provider, "HOST_SITES", {})

NORMALIZE_SITE_URL_PATTERN = getattr(_provider, "NORMALIZE_SITE_URL_PATTERN", r"(?!)")
SITE_PATH_PATTERN = getattr(_provider, "SITE_PATH_PATTERN", r"(?!)")
