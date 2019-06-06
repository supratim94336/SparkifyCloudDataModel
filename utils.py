import sys
import time


def animate():
    chars = r"|/—\|"
    for char in chars:
        sys.stdout.write('\r' + 'Please Wait ...' + char)
        time.sleep(.1)
        sys.stdout.flush()
