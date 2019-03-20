import numpy as np, os, sys

tim = sys.argv[1]
nam = sys.argv[2]

cmd1 = "echo "+tim+"-"+nam+"- | nc -4u -w1 10.10.1.1 11224 &"
cmd2 = "echo "+tim+"-"+nam+"- | nc -4u -w1 10.10.1.7 11224 &"
cmd3 = "echo "+tim+"-"+nam+"- | nc -4u -w1 10.10.1.8 11224 &"
cmd4 = "echo "+tim+"-"+nam+"- | nc -4u -w1 10.10.1.9 11224 &"
cmd5 = "echo "+tim+"-"+nam+"- | nc -4u -w1 10.10.1.10 11224 &"

os.system(cmd1)
os.system(cmd2)
os.system(cmd3)
os.system(cmd4)
os.system(cmd5)


