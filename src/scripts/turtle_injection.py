import asyncio
from cap.data.virtuoso import VirtuosoClient, VirtuosoConfig

async def inject_ontology_file():
    ontology_path = "src/ontologies/cardano.ttl"
    graph_uri = "http://cardano"
    if ontology_path != "":
        with open(ontology_path, "r") as f:
            turtle_data = f.read()
    else:
        turtle_data = ""

    vc = VirtuosoConfig(
        host="localhost",
        port=8890,
        username="dba",
        password="dba"
    )
    client = VirtuosoClient(vc)
    return await client._make_crud_request(
        method="POST",
        graph_uri=graph_uri,
        data=turtle_data
    )

if __name__ == "__main__":
    asyncio.run(inject_ontology_file())