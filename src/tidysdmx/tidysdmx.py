def fetch_dsd_schema(fmr_params: dict, env: str, dsd_id):
    """
    Fetches the Data Structure Definition (DSD) schema from a given Fusion Metadata Registry (FMR) URL.

    Args:
        fmr_params (dict): It has base url and endpoints to access FMR's API.
        env (str): FMR Environment to get the data from. It could be 'sandbox', 'qa', 'dev' or 'prod'.
        dsd_id (str): The identifier of the Data Structure Definition, typically in the format "agency:id(version)".

    Returns:
        dict: The schema of the requested Data Structure Definition.

    Raises:
        ValueError: If the URL is not syntactically valid.
        aiohttp.ClientError: If there is an issue with the HTTP request.
        px.io.exceptions.FormatError: If there is an issue with the format of the response.

    Example:
        schema = fetch_dsd_schema("https://example.com/fmr", "WB:WDI(1.0)")
    """
    format = px.io.format.StructureFormat.FUSION_JSON

    fmr_url = fmr_params[env]["url"]

    # Ensure the URL is syntactically valid
    base_url = urljoin(fmr_url, "/FMR/sdmx/v2/")

    client = fmr.RegistryClient(
        base_url,
        format=format,
    )

    agency, id, version = parse_dsd_id(dsd_id)
    schema = client.get_schema("datastructure", agency, id, version)
    return schema


def parse_dsd_id(dsd_id):
    """
    Parses the Data Structure Definition (DSD) identifier into its components.

    Args:
        dsd_id (str): The identifier of the Data Structure Definition, typically in the format "agency:id(version)".

    Returns:
        tuple: A tuple containing the agency, id, and version.

    Raises:
        ValueError: If the dsd_id is not in the expected format.
    """
    try:
        agency, rest = dsd_id.split(":")
        id, version = rest.split("(")
        version = version.rstrip(")")
        return agency, id, version
    except ValueError:
        raise ValueError("Invalid dsd_id format. Expected format: 'agency:id(version)'")