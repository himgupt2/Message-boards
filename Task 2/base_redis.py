import redis
dir(redis)

r = redis.Redis()


subscribing = False;
topic = "";

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
		if cmd_parts[0] == "set":
			to_set = ' '.join(cmd_parts[2:])
			r.set(cmd_parts[1], to_set)
		elif cmd_parts[0] == "get":
			res = r.get(cmd_parts[1]) 
			print (res)
		elif cmd_parts[0] == "pub":
			to_pub = ' '.join(cmd_parts[2:])
			res = r.publish(cmd_parts[1], to_pub) 
			print (res)
		elif cmd_parts[0] == "sub":
			subscribing = True;
			p = r.pubsub()
			res = p.subscribe([cmd_parts[1]]) 
			print (res)
		elif cmd_parts[0] == "quit":
			break;
		else:
			print("Input format wrong");

	except KeyboardInterrupt:
		subscribing = False
