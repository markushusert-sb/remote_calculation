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
from collections import defaultdict
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
    parser.add_argument('time',type=float,help="in days, how old downloaded calculations are allowed to be",const=28.,nargs='?',default=28.)
    parser.add_argument('--force','-f', action='store_true',help='force download of files even if already done')
    parser.add_argument('--ntries', type=int,default=3,help='force download of files')
    args = parser.parse_args()

    return args
def main():
    subprocess.run(f"touch ${download_file}",shell=True,capture_output=True)
    args=parse_cmd_line()
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.remote_logging_file),"r") as fil:
        lines=fil.readlines()
    already_aborted_file=os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.already_aborted_file)
    status_dict=defaultdict(list)
    counter=0
    done_paths=set()
    for line in lines:
        components=line.split(" ")
        date=components[0]
        hour=components[1]
        time=datetime.strptime(f"{date}",'%Y-%m-%d')
        remote_path=components[-1].split(":",1)[1].strip()
        host=components[-1].split(":",1)[0].strip()
        path=os.path.join(settings.local_anchor,os.path.relpath(remote_path,settings.remote_anchor))
        print(f'checking calculation in ={path}')
        if not os.path.isdir(path) or path in done_paths:
           continue 
        done_paths.add(path)
        now = datetime.now() 
        diff=now-time
        if diff.days <=args.time:
            counter+=1
            local_args=remote_calc.read_cache_data(path)
            try:
                code=remote_calc.download_results(path,argparse.Namespace(action='c',force=args.force))

            except remote_calc.SSHError:
                break
            if 'aborted' in code and local_args['number_tries']<args.ntries:
                print('relaunching calculation')
                remote_calc.setup_and_run_job_remotely(argparse.Namespace(action='r',job=path),path)
                code='running'
            status_dict[code].append((path,remote_path,host))
    #print(status_dict['already_aborted']+status_dict['aborted'])

    with open(already_aborted_file,"r") as fil:
        existing=set([i.strip() for i in fil.readlines()])
    with open(already_aborted_file,"w") as fil:
        fil.write('\n'.join(existing.union([i[0] for i in status_dict['aborted']])))
    print(f"\nSummary of {counter} downloads:\n")
    for code in ['done','already','running','already_aborted','aborted']:
        paths=status_dict[code]
        if code=='already':
            print(f"{len(paths)} calculations had already been downloaded")
        if code=='already_aborted':
            print(f"{len(paths)} calculations had already been detected as aborted, you may see a list of all aborted jobs in {already_aborted_file}")
        if code=='running':
            print(f"{len(paths)} calculations are still running, namely")
            for path,remote_path,host in paths:
                print(path)
        if code=='aborted':
            print(f"{len(paths)} calculations have been aborted prematurely, namely:")
            for path,remote_path,host in paths:
                print('- '+path)
        if code=='done':
            print(f"{len(paths)} calculations have just been downloaded")
            for path,remote_path,host in paths:
                print('- '+path)


if __name__=="__main__":
    main()
