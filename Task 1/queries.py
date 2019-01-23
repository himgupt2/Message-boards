from mongo_connect import connectMongo
import constants
import pymongo
import json
import pprint

collection = connectMongo()
collection.remove()

with open('initial.json') as f:
    data = json.load(f)
    collection.insert_many(data)

######## INSERT ENTRIES ########
with open('dummy-fitness.json') as f:
    data = json.load(f)
    collection.insert_many(data)

with open('user1001-new.json') as f:
    data = json.load(f)
    getUID = {"uid" : data["uid"]}
    setData = {"$set" : data}
    collection.update_many(getUID, setData)

##### FIND ALL ENTRIES IN THE DATABASE #####
# Assuming RQ0 is the query to find all entries in the database
# RQ0 = collection.find()
# for data in RQ0:
#     pprint.pprint(data)

print("<-------RQ1------>")

RQ1 = collection.find()
print("Total Count: ",collection.count())
print()

print("<-------RQ2------>")
print("Employees tagged as irregular")
print()
RQ2 = collection.find({"tags": "irregular"})
print ("Object ID                UID")
for data in RQ2:
    print( data["_id"], data["uid"])

print()

print("<-------RQ3------>")
print("Step Goal less than equal to 1500")
RQ3 = collection.find({"goal.stepGoal": { "$lte": 1500}})
print ("Object ID                UID")
for data in RQ3:
    print( data["_id"], data["uid"])

print()
######## AGGREGATE ENTRIES WITH PIPELINE ########
print("<-------RQ4------>")
print("activityDuration for each employee")
RQ4 = collection.aggregate([ {"$project": {"_id": "$uid", "activityDuration": {"$sum": "$activityDuration"}  } }])
for data in RQ4:
    pprint.pprint(data)