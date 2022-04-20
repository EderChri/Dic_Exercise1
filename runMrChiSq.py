from MrChiSq import MrChiSq

# This script runs the second Job and prints the last line containing unique words

# Creates MrChiSq object
mrchi = MrChiSq()
with mrchi.make_runner() as runner:
    runner.run()
    # Key value pairs to output
    for key, value in mrchi.parse_output(runner.cat_output()):
        print(key, value, end='')