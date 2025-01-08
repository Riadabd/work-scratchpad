import uuid
import os
import json

from SPARQLWrapper import SPARQLWrapper, JSON

"""
The template provides the user with several helper methods. They aim to give you a step ahead for:

- logging
- JSONAPI-compliancy
- SPARQL querying

The below helpers can be imported from the `helpers` module. For example:
```py
from helpers import *
```

Available functions:
"""


def generate_uuid():
    """Generates a random unique user id (UUID) based on the host ID and current time"""
    return str(uuid.uuid4())


def error(msg, status=400, **kwargs):
    """
    Returns a Response object containing a JSONAPI compliant error response with the given status code (400 by default).

    Response object documentation: https://flask.palletsprojects.com/en/1.1.x/api/#response-objects
    The kwargs can be any other key supported by JSONAPI error objects: https://jsonapi.org/format/#error-objects
    """
    error_obj = kwargs
    error_obj["detail"] = msg
    error_obj["status"] = status
    response = json.dumps({"errors": [error_obj]})
    response.status_code = error_obj["status"]
    response.headers["Content-Type"] = "application/vnd.api+json"
    return response


def validate_json_api_content_type(request):
    """Validate whether the request contains the JSONAPI content-type header (application/vnd.api+json). Returns a 404 otherwise"""
    if "application/vnd.api+json" not in request.content_type:
        return error(
            "Content-Type must be application/vnd.api+json instead of "
            + request.content_type
        )


def validate_resource_type(expected_type, data):
    """Validate whether the type specified in the JSON data is equal to the expected type. Returns a `409` otherwise."""
    if data["type"] is not expected_type:
        return error(
            "Incorrect type. Type must be "
            + str(expected_type)
            + ", instead of "
            + str(data["type"])
            + ".",
            409,
        )


sparqlQuery = SPARQLWrapper(os.environ.get("MU_SPARQL_ENDPOINT"), returnFormat=JSON)
sparqlUpdate = SPARQLWrapper(os.environ.get("MU_SPARQL_UPDATEPOINT"), returnFormat=JSON)
sparqlUpdate.method = "POST"
if os.environ.get("MU_SPARQL_TIMEOUT"):
    timeout = int(os.environ.get("MU_SPARQL_TIMEOUT"))
    sparqlQuery.setTimeout(timeout)
    sparqlUpdate.setTimeout(timeout)


def query(the_query):
    """Execute the given SPARQL query (select/ask/construct) on the triplestore and returns the results in the given return Format (JSON by default)."""

    sparqlQuery.setQuery(the_query)
    return sparqlQuery.query().convert()


def update(the_query):
    """Execute the given update SPARQL query on the triplestore. If the given query is not an update query, nothing happens."""
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        sparqlUpdate.query()
