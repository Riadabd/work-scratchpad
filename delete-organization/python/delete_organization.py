import csv
import re
from datetime import datetime, timedelta

from helpers.sparql_helpers import query
from helpers.helpers import (
    write_select_query,
    write_delete_query,
    has_direct_forward_properties,
    write_direct_forward_delete_query,
    has_direct_reverse_properties,
    write_direct_reverse_delete_query,
    write_clear_graph_query,
    is_graph_populated,
    graph_contains_data_source_property,
    write_graph_delete_query,
    is_session_graph_populated_for_org,
    write_session_delete_query
)
from helpers.helpers import ROLES
from config.loket.path_to_types import data as data_loket
from config.op.path_to_types import data as data_op

"""
One approach is to have a configuration file that details the links between the different types we have to delete.

Only composite types will be included. That is, types that are also "objects" containing data that needs deletion. One example
is a representative body in time that points to a representative body, which in turn is also an entity that contains predicates and
objects.

For every other type, we will automatically be writing `?s ?p ?o .`, which should take care of deleting them.

Organizations URIs are stored inside `config/organization_uris.txt`, and we go over them one by one.
"""

"""
Another approach is to represent the organization and its related entities in code (similar to what is done with an ORM). This way
we can define the links between each entity and walk through the links.

To quickly start iterating on the idea, the goal would be to only define the `children` and `reverseChildren`. Regular properties will
normally be deleted through the `?p ?o` link.
"""


def write_delete_queries(organization_uri: str, filename: str) -> None:
    file = open(f"output/loket/{filename}", "w")

    for item in data_loket["delete"]:
        q: str = write_select_query(
            organization_uri,
            item["type"],
            item["pathToType"],
            item["additionalFilter"],
        )
        results = query(q)
        uris: list[str] = []

        for result in results["results"]["bindings"]:
            uris.append(result["resource"]["value"])

        if uris:
            file.write(write_delete_query(uris, item["additionalFilter"]) + "\n;\n")

    for role in ROLES:
        if query(is_graph_populated(organization_uri, role))["boolean"]:
            # If graph is populated, delete its contents.
            file.write(write_graph_delete_query(organization_uri, role) + "\n;\n")

    if query(is_session_graph_populated_for_org(organization_uri))["boolean"]:
        file.write(write_session_delete_query(organization_uri) + "\n;\n")

    # Delete direct forward properties belonging to the organization
    file.write(write_direct_forward_delete_query(organization_uri) + "\n;\n")

    # Delete direct reverse properties pointing to the organization
    file.write(write_direct_reverse_delete_query(organization_uri))

    file.close()

def write_delete_queries_op(organization_uri: str, filename: str) -> None:
    output: list[str] = []

    for item in data_op["delete"]:
        q: str = write_select_query(
            organization_uri,
            item["type"],
            item["pathToType"],
            item["additionalFilter"],
        )

        results = query(q)
        uris: list[str] = []

        for result in results["results"]["bindings"]:
            uris.append(result["resource"]["value"])

        if uris:
            output.append(write_delete_query(uris, item["additionalFilter"]))

    if query(has_direct_forward_properties(organization_uri))["boolean"]:
        # Delete direct forward properties belonging to the organization
        output.append(write_direct_forward_delete_query(organization_uri))

    if query(has_direct_reverse_properties(organization_uri))["boolean"]:
        # Delete direct reverse properties pointing to the organization
        output.append(write_direct_reverse_delete_query(organization_uri))

    if output:
        with open(f"output/op/{filename}", "w") as file:
            file.write("\n;\n".join(output))

if __name__ == "__main__":
    with open("config/organization_uris.csv", "r") as file:
        csv_reader = csv.reader(file)
        current = datetime.now()

        # Skip header
        next(csv_reader)
        for row in csv_reader:
            # Strip characters such as -,:,; and spaces (one or more)
            name: str = re.sub(r"[-:\s+]", " ", row[1])
            filename: str = (
                f"{current.strftime("%Y%m%d%H%M%S")}-delete-{'-'.join(name.lower().split())}.sparql"
            )

            # write_delete_queries(row[0], filename)
            write_delete_queries_op(row[0], filename)

            # Formatting now() on every time iteration will yield duplicate output since
            # we only capture seconds. For exammple, we may get "20250109195251" as the time
            # prefix for multiple files before the clock moves one second, and the output changes.
            current = current + timedelta(seconds=1)
