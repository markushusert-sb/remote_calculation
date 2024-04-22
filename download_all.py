#!/usr/bin/env python3
import argparse
from pathlib import Path
import glob
import subprocess
import sys
import os
import json
import logging_remote
import remote_calc
import sys
from datetime import datetime
from datetime import timedelta
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),"mods"))
try:
    import usersettings as settings
except ImportError:
    import settings
sys.path.pop(-1)
def parse_cmd_line():
    parser = argparse.ArgumentParser(description='serves to launch simulations on a cluster and leave behind a trace permitting to download results automatically later')
    parser.add_argument('time',type=float,help="in days, how old downloaded calculations are allowed to be")
    args = parser.parse_args()

    return args
def main():
    args=parse_cmd_line()
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.remote_logging_file),"r") as fil:
        lines=fil.readlines()
    for line in lines:
        components=line.split(" ")
        date=components[0]
        hour=components[1]
        time=datetime.strptime(f"{date}",'%Y-%m-%d')
        path=os.path.join(settings.local_anchor,os.path.relpath(components[-1].split(":",1)[1].strip(),settings.remote_anchor))
        now = datetime.now() 
        diff=now-time
        if diff.days <=args.time:
            local_args=remote_calc.read_cache_data(os.path.join(path,settings.cache_file))
            remote_calc.download_results(path,argparse.Namespace(action='c'))

if __name__=="__main__":
    main()
