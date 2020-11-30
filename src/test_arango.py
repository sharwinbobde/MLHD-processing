import pyArango as arango
from arango import ArangoClient
import config

if __name__ == '__main__':
    print("arango")
    client = ArangoClient(hosts='http://localhost:8529')
    # db = conn.createDatabase("py_test")
    db = client.db('py_test', username=config.arangodb_user, password=config.arangodb_password)
    colA = db.collection('A')
    colB = db.collection('B')

    if db.has_graph('food_system'):
        graph = db.graph('food_system')
    else:
        graph = db.create_graph('food_system')

    # if graph.has_edge_collection("food_system"):
    #     colEats = graph.edge_collection('eats')
    # else:
    #     colEats = graph.create_edge_definition(
    #         edge_collection='eats',
    #         from_vertex_collections=["A"],
    #         to_vertex_collections=["B"])

    names = ["sharwin", "nikhil", "anagha", "yurij"]
    foods = ["pasta", "pida", "noodles", "chicken"]

    for name in names:
        try:
            colA.insert({"_key": name})
        except:
            print("A present")

    for food in foods:
        try:
            colB.insert({"_key": food})
        except:
            print("B present")

    for name in names:
        for food in foods:
            doc = {"_from": "A/" + name, "_to": "B/" + food}
            db.aql.execute("insert " + str(doc) + "into eats")
            # if not colEats.has(doc):
            #     colEats.insert(doc)
