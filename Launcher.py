import json
import random
from DBTools import printJSONDB
from DBTools import loadDB
from DBTools import shortestPath

#Make sure orientDB version 2.2 is running, version 3.x does not work with these drivers

#-d run in background login:root password:rootpwd
#docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:2.2

#-it run in foreground login:root password:rootpwd
#docker run -it --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:2.2

#path to json file
filepath = 'master.json'

#example of how to parse elements from JSON file
# printJSONDB(filepath)

#loadDB with JSON data, removing existing database if it exist
#comment the load command after the database is loaded
loadDB(filepath)

#load the JSON database so we can randomly pick a key for shortest path testing
with open(filepath) as f:
    data = json.load(f)

#Cody's JSON id is 1, because why not?
codyid = 1

#grab a random identifier
randomPerson = random.choice(list(data.keys()));

#Nicolaus Copernicus JSON id is 126177
#Distance between Cody and Copernicus
print(shortestPath(codyid,126177))

#Gregory Palamas JSON id is 176844
#Distance between Cody and Palamas
print(f"Distance between Cody and Palamas-san: {shortestPath(codyid,176844)}")

#Distance between Cody and some rando
print(shortestPath(codyid,randomPerson))
#print rando info
print(data.get(randomPerson))
