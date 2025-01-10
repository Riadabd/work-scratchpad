data = {
    "delete": [
        # Bestuursorgaan
        {
            "type": "http://data.vlaanderen.be/ns/besluit#Bestuursorgaan",
            "pathToType": """
                ?resource <http://data.vlaanderen.be/ns/besluit#bestuurt> ?organization .
            """,
            "additionalFilter": "",
        },
        # Bestuursorgaan in tijd
        {
            "type": "http://data.vlaanderen.be/ns/besluit#Bestuursorgaan",
            "pathToType": """
                ?resource <https://data.vlaanderen.be/ns/generiek#isTijdspecialisatieVan> ?bestuursorgaan .
                ?bestuursorgaan <http://data.vlaanderen.be/ns/besluit#bestuurt> ?organization .
            """,
            "additionalFilter": "",
        },
        # Sites
        {
            "type": "http://www.w3.org/ns/org#Site",
            "pathToType": """
                ?organization <http://www.w3.org/ns/org#hasPrimarySite> ?resource .
            """,
            "additionalFilter": """FILTER (?g != <http://mu.semte.ch/graphs/landing-zone/clb-contact-data>)"""
        }
    ]
}
