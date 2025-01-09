import csv
import re
from datetime import datetime

from helpers.sparql_helpers import query
from helpers.helpers import (
    write_select_query,
    write_delete_query,
    write_direct_forward_delete_query,
    write_direct_reverse_delete_query,
    is_graph_populated
)
from helpers.helpers import ROLES
from config.loket.path_to_types import data
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
    file = open(f"output/{filename}", "w")

    for index, item in enumerate(data["delete"]):
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

    # Delete direct forward properties belonging to the organization
    file.write(write_direct_forward_delete_query(organization_uri))

    # Delete direct reverse properties pointing to the organization
    file.write(write_direct_reverse_delete_query(organization_uri))

    file.close()

def write_delete_queries_op(organization_uri: str, filename: str) -> None:
    file = open(f"output/op/{filename}", "w")

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
            file.write(write_delete_query(uris, item["additionalFilter"]) + "\n;\n")

    # Delete direct forward properties belonging to the organization
    file.write(write_direct_forward_delete_query(organization_uri))

    # Delete direct reverse properties pointing to the organization
    file.write(write_direct_reverse_delete_query(organization_uri))

    file.close()

if __name__ == "__main__":
    with open("config/organization_uris.csv", "r") as file:
        csv_reader = csv.reader(file)

        # Skip header
        next(csv_reader)
        for row in csv_reader:
            current_datetime: str = datetime.now().strftime("%Y%m%d%H%M%S")
            # Strip characters such as -,:,; and spaces (one or more)
            name: str = re.sub(r"[-:\s+]", " ", row[1])
            filename: str = (
                f"{current_datetime}-delete-{'-'.join(name.lower().split())}.sparql"
            )

            write_delete_queries(row[0], filename)
