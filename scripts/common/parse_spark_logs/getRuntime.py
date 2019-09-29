#!/usr/bin/env python3

from api import *
import sys

runtime = getAppDurationById(sys.argv[1]);
print(runtime)
