import redis
dir(redis)

from mongo_connect import connectMongo
import constants
import pymongo
import json
import pprint

r = redis.Redis()

collection = connectMongo()

subscribing = False;
topic = "";
print ("\n <======= Following commands can be used =====>\n")
print ("1. To select Message Board: select [fit]")
print ("2. To write data to Message Board: write [data]")
print ("3. To read data from Message Board: read ")
print ("4. To listen to updates from other users: listen ")
print ("5. To stop listening to Updates: <Ctrl+C> ")
print ("6. To exit the application: quit or Quit\n ")

#collection.remove()
RQ = collection.find()
if(collection.count()==0):
	print("No Message Boards found")
else:
	print("Following Message Boards found: ")
	for data in RQ:
		pprint.pprint(data["_id"])

while True:
	try:
		if subscribing:
			print("Sub")
			for item in p.listen():
				print(item)

		cmd = input('Enter your command: ')
		print(cmd)
		cmd_parts = cmd.split(" ")
		print(cmd_parts)
		if cmd_parts[0] == "select":
			topic = cmd_parts[1]
		elif cmd_parts[0] == "read":
			if(topic):
				RQ = collection.find({ "_id": topic })
				for data in RQ:
					pprint.pprint(data)
			else:
				print ("Please select a message board from the list")
		elif cmd_parts[0] == "write":
			to_set = (' '.join(cmd_parts[1:])).strip(' \t\n\r')
			RQ = collection.find({'$and': [{"_id": topic} ,  { "_msgs": {'$exists': True} } ]})
			if(topic):
				if(to_set):
					print (to_set)
					if (RQ.count() == 0):
						collection.insert([{ '_id' : topic, '_msgs' : [to_set] }])
					else:
						collection.update({ "_id": topic }, { '$push': { "_msgs": to_set } } )
				else:
					print("Empty message not allowed")
			else:
				print ("Please select a message board from the list")
			res = r.publish(topic, to_set) 
		elif cmd_parts[0] == "listen":
			if(topic):
				subscribing = True;
				p = r.pubsub()
				res = p.subscribe([topic]) 
				print (res)
			else:
				print ("Please select a message board from the list")
		elif cmd_parts[0] == "quit" or cmd_parts[0] == "exit" or cmd_parts[0] == "Quit" :
			break;
		else:
			print("Input format wrong");

	except KeyboardInterrupt:
		subscribing = False