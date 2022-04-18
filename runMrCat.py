from MapRedChiSq import MrCat

mrcat = MrCat()
with mrcat.make_runner() as runner:
    runner.run()
    N = runner.counters()[0]['mapper calls']['counter']
    catCount = runner.counters()[0]['catcounter']
    f = open("CatCount.txt", "w")
    f.write(str(N)+"\n")
    f.write(str(catCount))
    f.close()