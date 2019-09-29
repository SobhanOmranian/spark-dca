#!/usr/bin/env python3

from api import *
import sys

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-a", "--appId")
parser.add_argument("-s", "--stageId")

args = parser.parse_args(sys.argv[1:])

appId = args.appId
stageId = args.stageId

print(getStageDurationForApp(appId, stageId))
