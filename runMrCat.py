from MrCat import MrCat

# This script runs the first Job and extracts the counter variables
# Furthermore it saves the extracted values into a text file

# Creates MrCat object
mrcat = MrCat()
with mrcat.make_runner() as runner:
    runner.run()
    # Extract Counters from runner
    N = runner.counters()[0]['mapper calls']['counter']
    catCount = runner.counters()[0]['catcounter']
    # Save into local file
    f = open("CatCount.txt", "w")
    f.write(str(N)+"\n")
    f.write(str(catCount))
    f.close()
    # Key value pairs to output
    for key, value in mrcat.parse_output(runner.cat_output()):
        print(key, value, "\n", end='')