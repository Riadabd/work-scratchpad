data = {
    "delete": [
        {
            "type": "http://data.vlaanderen.be/ns/besluit#Bestuursorgaan",
            "pathToType": """
                ?resource <http://data.vlaanderen.be/ns/besluit#bestuurt> ?organization .
            """,
            "additionalFilter": """FILTER(?g != <http://mu.semte.ch/graphs/landing-zone/op-public>)""",
        },
        {
            "type": "http://data.vlaanderen.be/ns/besluit#Bestuursorgaan",
            "pathToType": """
                ?resource <https://data.vlaanderen.be/ns/generiek#isTijdspecialisatieVan> ?bestuursorgaan .
                ?bestuursorgaan <http://data.vlaanderen.be/ns/besluit#bestuurt> ?organization .
            """,
            "additionalFilter": """FILTER(?g != <http://mu.semte.ch/graphs/landing-zone/op-public>)""",
        },
        # Covers the cases when ?p = <http://schema.org/recipient> or ?p = <http://schema.org/sender>
        {
            "type": "http://schema.org/Message",
            "pathToType": """
                ?resource ?p ?organization
            """,
            "additionalFilter": "",  # Messages can point to files (that relationship will be inside the public graph), so we do not specify any graph filter to delete this relationship.
        },
        # Files connected to messages
        {
            "type": "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject",
            "pathToType": """
                ?message a <http://schema.org/Message> ;
                    ?p ?organization ;
                    <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#hasPart> ?resource .
            """,
            "additionalFilter": "",
        },
        # Conversations pointing to messages
        {
            "type": "http://schema.org/Conversation",
            "pathToType": """
                ?message a <http://schema.org/Message> ;
                    ?p ?organization .

                ?resource <http://schema.org/hasPart> ?message .
            """,
            "additionalFilter": "",
        },
        # Employee datasets, period slices and observations
        {
            "type": "http://lblod.data.gift/vocabularies/employee/EmployeeDataset",
            "pathToType": """
                ?resource <http://purl.org/dc/terms/creator> ?organization .
            """,
            "additionalFilter": "",
        },
        {
            "type": "http://lblod.data.gift/vocabularies/employee/EmployeePeriodSlice",
            "pathToType": """
                ?dataset a <http://lblod.data.gift/vocabularies/employee/EmployeeDataset> ;
                    <http://purl.org/dc/terms/creator> ?organization ;
                    <http://purl.org/linked-data/cube#slice> ?resource .
            """,
            "additionalFilter": "",
        },
        {
            "type": "http://lblod.data.gift/vocabularies/employee/EmployeeObservation",
            "pathToType": """
                ?dataset a <http://lblod.data.gift/vocabularies/employee/EmployeeDataset> ;
                    <http://purl.org/dc/terms/creator> ?organization ;
                    <http://purl.org/linked-data/cube#slice> ?slice .

                ?slice <http://purl.org/linked-data/cube#observation> ?resource .
            """,
            "additionalFilter": "",
        },
        # BBCDR reports
        {
            "type": "http://mu.semte.ch/vocabularies/ext/bbcdr/Report",
            "pathToType": """
                ?resource <http://purl.org/dc/terms/subject> ?organization .
            """,
            "additionalFilter": """FILTER (?p != <http://mu.semte.ch/vocabularies/ext/bbcdr/package>)""", #  Preseve the package property as it helps with finding dangling files if want to deal with them at a later stage.
        },
        # Files connected to BBCDR reports
        {
            "type": "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject",
            "pathToType": """
                ?report a <http://mu.semte.ch/vocabularies/ext/bbcdr/Report> ;
                    <http://purl.org/dc/terms/subject> ?organization ;
                    <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#hasPart> ?resource .
            """,
            "additionalFilter": "",
        },
        # Submissions
        {
            "type": "http://rdf.myexperiment.org/ontologies/base/Submission",
            "pathToType": """
                ?resource <http://purl.org/pav/createdBy> ?organization .
            """,
            "additionalFilter": "",
        },
        # Submission documents
        {
            "type": "http://mu.semte.ch/vocabularies/ext/SubmissionDocument",
            "pathToType": """
                ?submission a <http://rdf.myexperiment.org/ontologies/base/Submission> ;
                    <http://purl.org/pav/createdBy> ?organization .

                ?submission <http://purl.org/dc/terms/subject> ?resource .
            """,
            # The source property can look as follows:
            #   dcterms:source	<share://submissions/268de021-7a18-11ee-a11e-5b20eafcb793.ttl> , <share://submissions/26292ae1-7a18-11ee-a11e-5b20eafcb793.ttl> , <share://submissions/26347581-7a18-11ee-a11e-5b20eafcb793.ttl>
            # Preseve the source property as it helps with finding dangling files if want to deal with them at a later stage.
            "additionalFilter": """FILTER (?p != <http://purl.org/dc/terms/source>)""",
        },
        # Files connected to submissions that have the following prefix:
        #   <http://mu.semte.ch/services/file-service/files/>
        {
            "type": "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject",
            "pathToType": """
                ?submission a <http://rdf.myexperiment.org/ontologies/base/Submission> ;
                    <http://purl.org/pav/createdBy> ?organization .

                ?submission <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#hasPart> ?resource .
            """,
            "additionalFilter": "",
        },
    ]
}
