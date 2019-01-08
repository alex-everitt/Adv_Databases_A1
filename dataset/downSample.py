with open("ratings.dat", "r") as source:
    lines = [line for line in source]
	
	
import random
random_choice = random.sample(lines, 60000)


with open("netIDs.data", "w") as sink:
    sink.write("".join(random_choice))	