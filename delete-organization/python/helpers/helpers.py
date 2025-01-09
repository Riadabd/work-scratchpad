from pathlib import Path

from helpers.sparql_escape_helpers import sparql_escape_uri

ROLES: list[str] = [
    "", # For the org graph
    "/LoketLB-toezichtGebruiker",
    "/LoketLB-personeelsbeheer",
    "/LoketLB-berichtenGebruiker"
]

def create_output_dir(path: str):
    folder_path = Path(path)
    folder_path.mkdir(parents=True, exist_ok=True)


def write_select_query(
    org_uri: str, type: str, path_to_type: str, additional_filter: str = None
) -> str:
    return f"""
SELECT DISTINCT ?resource WHERE {{
    BIND({sparql_escape_uri(org_uri)} AS ?organization)

    ?resource a {sparql_escape_uri(type)} .
    {path_to_type}
}}
"""


def write_direct_forward_delete_query(org_uri: str) -> str:
    return f"""
DELETE {{
  GRAPH ?g {{
    ?organization ?p ?o .
  }}
}}
WHERE {{
  BIND({sparql_escape_uri(org_uri)} AS ?organization)

  GRAPH ?g {{
    ?organization ?p ?o .
  }}
}}
"""

def write_direct_reverse_delete_query(org_uri: str) -> str:
    return f"""
DELETE {{
  GRAPH ?g {{
    ?s ?p ?organization .
  }}
}}
WHERE {{
  BIND({sparql_escape_uri(org_uri)} AS ?organization)

  GRAPH ?g {{
    ?s ?p ?organization .
  }}
}}
"""


def write_delete_query(uri: list[str], graph_filter: str, graph: str = None) -> str:
    if not graph:
        return f"""
DELETE {{
  GRAPH ?g {{
    ?s ?p ?o .
  }}
}}
WHERE {{
  VALUES ?s {{
    {'\n    '.join([sparql_escape_uri(x) for x in uri])}
  }}

  GRAPH ?g {{
    ?s ?p ?o .
  }}

  {graph_filter}
}}
"""
    else:
        return f"""
DELETE {{
  GRAPH {sparql_escape_uri(graph)} {{
    ?s ?p ?o .
  }}
}}
WHERE {{
  VALUES ?s {{
    {'\n    '.join([sparql_escape_uri(x) for x in uri])}
  }}

  GRAPH {sparql_escape_uri(graph)} {{
    ?s ?p ?o .
  }}

  {graph_filter}
}}
"""
