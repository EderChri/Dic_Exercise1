from MrChiSq import MrChiSq

mrchi = MrChiSq()
with mrchi.make_runner() as runner:
    runner.run()
    for key, value in mrchi.parse_output(runner.cat_output()):
        print(key, value, end='')
    print(str(sorted(list(mrchi.words)))[1:-1].replace(' ', '').replace("'",'').replace(',', ' '))