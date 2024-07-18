#!/usr/bin/env python3
import argparse
from pathlib import Path
import glob
import subprocess
import sys
import os
import json
import logging_remote
import time
import shutil
import remote_calc
from collections import defaultdict
import sys
from datetime import datetime
from datetime import timedelta
log=logging_remote.standart_logger(__name__)
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),"mods"))
try:
    import usersettings as settings
except ImportError:
    import settings
sys.path.pop(-1)
def parse_cmd_line():
    parser = argparse.ArgumentParser(description='serves to launch simulations on a cluster and leave behind a trace permitting to download results automatically later')
    parser.add_argument('dir',type=os.path.abspath,help="directory in which to download calculations, if not given remote_log_file is read",default=None,nargs='?')
    parser.add_argument('--time',type=float,help="in days, how old downloaded calculations are allowed to be if reading them from log-file",const=28.,nargs='?',default=28.)
    parser.add_argument('--force','-f', action='store_true',help='force download of files even if already done')
    parser.add_argument('--ntries', type=int,default=7,help='force download of files')
    args = parser.parse_args()

    return args
def get_regular_exec_file():
    return os.path.join(os.environ['regular_commands_dir'],os.path.basename(__file__))
def determine_dirs_to_check(args):
    to_check=[]
    done_paths=set()
    if args.dir is None:
       to_check=remote_calc.get_calculations_older_than_x_hours(args.time*24,rewrite=True) 
    else:
       cache_files=remote_calc.find_results_in_dir(args.dir,remote_calc.settings.cache_file) 
       to_check=[os.path.dirname(i) for i in cache_files]
    log.info(f'number of calculations to check={len(to_check)}')
    return to_check

def main():
    #to mark time of last download
    if 'download_file' in os.environ:
        subprocess.run(f"touch {os.environ['download_file']}",shell=True,capture_output=True)
    args=parse_cmd_line()
    dirs=determine_dirs_to_check(args)
    already_aborted_file=os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.already_aborted_file)
    iterator=iter(dirs)
    path=next(iterator,'')
    counter=0
    status_dict=defaultdict(list)
    while path:
        counter+=1
        local_args=remote_calc.read_cache_data(path)
        log.info(f'checking {path}')
        if  'host' not in local_args:
            path=next(iterator,'')
            continue
        host=local_args['host']
        try:
            code=remote_calc.download_results(path,argparse.Namespace(action='c',force=args.force))
            if (code == 'submitted' or 'aborted' in code) and local_args['number_tries']<args.ntries:
                log.info('relaunching calculation')
                remote_calc.setup_and_run_job_remotely(argparse.Namespace(action='r',job=path),path)
                code='restarted'
            status_dict[code].append((path,host))
        except remote_calc.SSHError:
            log.info(f'stopping downloads due to communication error, waiting {remote_calc.limit_communication_blockage_minutes} minutes to retry')
            time.sleep(remote_calc.limit_communication_blockage_minutes*60)
            continue
        path=next(iterator,'')
    #log.info(status_dict['already_aborted']+status_dict['aborted'])
    if os.path.isfile(already_aborted_file):
        with open(already_aborted_file,"r") as fil:
            existing=set([i.strip() for i in fil.readlines()])
    else:
        existing=set() 
    with open(already_aborted_file,"w") as fil:
        fil.write('\n'.join(existing.union([i[0] for i in status_dict['aborted']])))
    log.info(f"\nSummary of {counter} downloads:\n")
    summary=dict()
    for code in ['done','already','running','already_aborted','aborted','restarted']:
        paths=status_dict[code]
        summary[code]=len(paths)
        if code=='already':
            log.info(f"{len(paths)} calculations had already been downloaded")
        if code=='already_aborted':
            log.info(f"{len(paths)} calculations had already been detected as aborted, you may see a list of all aborted jobs in {already_aborted_file}")
        if code=='running':
            log.info(f"{len(paths)} calculations are still running, namely")
            for path,host in paths:
                log.info(path)
        if code=='aborted':
            log.info(f"{len(paths)} calculations have been aborted prematurely, namely:")
            for path,host in paths:
                log.info('- '+path)
        if code=='done':
            log.info(f"{len(paths)} calculations have just been downloaded, namely:")
            for path,host in paths:
                log.info('- '+path)
        if code=='restarted':
            log.info(f"{len(paths)} calculations have been restarted, namely:")
            for path,host in paths:
                log.info('- '+path)
    print(f"Summary of all checked calculations: "+";".join(f"{stat}={nr}" for stat,nr in summary.items()))
    if "regular_commands_dir" in os.environ and len(status_dict['running'])+len(status_dict['restarted'])==0:
        if os.path.isfile(get_regular_exec_file()):
            shutil.remove(get_regular_exec_file())
            
if __name__=="__main__":
    if "regular_commands_dir" in os.environ:
        subprocess.run(f"touch {get_regular_exec_file()}",shell=True,capture_output=True)
    main()
