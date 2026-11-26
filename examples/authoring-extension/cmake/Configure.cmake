option(RDF_ENABLE_EXAMPLE_AUTHORING "Enable the optional Python authoring example" ON)
rdf_register_extension(NAME example_authoring ENABLED "${RDF_ENABLE_EXAMPLE_AUTHORING}"
    TARGET ExampleAuthoring)
