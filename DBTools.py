import pyorient
import json


def reset_db(client, name):
    # Remove Old Database
    if client.db_exists(name):
        client.db_drop(name)

    # Create New Database
    client.db_create(name,
                     pyorient.DB_TYPE_GRAPH,
                     pyorient.STORAGE_TYPE_PLOCAL)


def getrid(client, id):
    nodeId = client.query("SELECT FROM V WHERE id = '" + str(id) + "'")
    return str(nodeId[0]._rid)


def printJSONDB(filepath):
    with open(filepath) as f:
        data = json.load(f)

    formatted_data = json.dumps(data,indent=2)
    print(formatted_data)


def loadDB(filepath):
    # database name
    dbname = "agen"
    # database login is root by default
    login = "root"
    # database password, set by docker param
    password = "rootpwd"

    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # remove old database and create new one
    reset_db(client, dbname)

    # open the database we are interested in
    client.db_open(dbname, login, password)

    # create class with all attributes
    client.command("CREATE CLASS Person EXTENDS V")
    client.command("CREATE PROPERTY Person.id Integer")
    client.command("CREATE PROPERTY Person.students EMBEDDEDLIST Integer")
    client.command("CREATE PROPERTY Person.advisors EMBEDDEDLIST Integer")
    client.command("CREATE PROPERTY Person.wikiURL String")
    client.command("CREATE PROPERTY Person.wikiImage String")
    client.command("CREATE PROPERTY Person.degreeLists EMBEDDEDMAP String")

    # open and parse local json file
    with open(filepath) as f:
        data = json.load(f)

    # loop through each key in the json database and create a new vertex, V with the id in the database
    for key,values in data.items():
        command=f"CREATE VERTEX Person SET "
        for attribute in ["id", "name", "students", "advisors", "wikiUrl", "wikiImage", "degreeLists"]:
            value = data.get(key).get(attribute)
            if value:
                command += f'attribute="{value}"'
        client.command(command)
        # print(key,values)
        # return
        # client.command("CREATE VERTEX Person SET id = '" + key + "'")

    # loop through each key creating edges from advisor to advisee
    for key in data:
        advisorNodeId = str(getrid(client, key))
        for student in data.get(key)["students"]:
            studentNodeId = str(getrid(client, student))
            client.command("CREATE EDGE FROM " + advisorNodeId + " TO " + studentNodeId)

    client.close()


def shortestPath(personIdA, personIdB):
    # personId[A/B] is the V id, which matches the id in the JSON file
    # personNodeId[A/B] is the internal OrientDB node ids (rid), which are used for functions like shortestPath

    dbname = "agen"
    login = "root"
    password = "rootpwd"

    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    client.db_open(dbname, login, password)

    # get the RID of the two people
    personNodeIdA = getrid(client, personIdA)
    personNodeIdB = getrid(client, personIdB)

    # determine the shortest path
    pathlist = client.command("SELECT shortestPath(" + personNodeIdA + ", " + personNodeIdB + ")")
    # print(pathlist[0].__getattr__('shortestPath'))

    # get distance
    distance = len(pathlist[0].__getattr__('shortestPath'))

    for node in pathlist[0].__getattr__('shortestPath'):
        print(node)

    # pyorient.otypes.OrientRecord
    client.close()
    return distance
