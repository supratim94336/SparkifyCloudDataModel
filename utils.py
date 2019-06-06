import sys
import time


def animate():
    sys.stdout.write('\rloading |')
    time.sleep(0.1)
    sys.stdout.write('\rloading /')
    time.sleep(0.1)
    sys.stdout.write('\rloading -')
    time.sleep(0.1)
    sys.stdout.write('\rloading \\')
    time.sleep(0.1)
