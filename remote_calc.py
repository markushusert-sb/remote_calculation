#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import timedelta
from datetime import datetime
import glob
import time
import subprocess
import sys
import os
import json
import logging_remote
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),"mods"))
try:
    import usersettings as settings
except ImportError:
    import settings
sys.path.pop(-1)
settings_not_to_update_locally={'host'}
class SSHError(Exception):
    def __init__(self, message):
        super().__init__(message)
parser = argparse.ArgumentParser(description='serves to launch simulations on a cluster and leave behind a trace permitting to download results automatically later')
parser.add_argument('action',type=str,choices=["r","c","b","s"],nargs="+",help="action to do for specified jobs, r(un),b(uild) filestructure or c(echk) and download if job is done, s(how) list of executed jobs")
parser.add_argument('--job',"-j",type=str,help="directory containing job to run, ignored if action is check",default=".")
parser.add_argument('--commands','-c',type=str,nargs='+',help='commands to execute on remote host')
parser.add_argument('--host', type=str,help='host to run on')
parser.add_argument('--upload','-u', type=str,nargs='+',help='files to upload, will be globbed in local dir',default=["*.mesh","*.edp"])
parser.add_argument('--download','-d', type=str,nargs='+',help='filepatterns to download')
parser.add_argument('--needed_gb', type=int,help='estimated gb needed to carry cout calulation. feel free to add safety factor. script will search inside of args.possible_hosts for host with sufficient memory',default=5)
parser.add_argument('--possible_hosts', type=str,nargs='+',help='lists of all admissible hosts for job. by default takes all of the machines',default=["keket","moreau","hermes","boch","hera","hades","xeller","poseidon"])
parser.add_argument('--force','-f', action='store_true',help='force download of files even if already downloaded')
parser.add_argument('--wait','-w', action="store_true",help='wait for calculation to be done?')
def parse_cmd_line():
    args = parser.parse_args()

    for key,val in list(vars(args).items()):
        if val is None:
            vars(args).pop(key)

    return args
def get_top_info(host):
    output=execute_commands_remotely(host,["top -b -n 1"],'').stdout.read().decode("utf-8")
    lines=output.split("\n")
    if len(''.join(lines))==0:
        return 0,0 #could not connect to host
    free_memory=int(lines[3][26:32])/1000.
    cpu=int(lines[2][36:38])
    return cpu,free_memory
def add_childjob(job,json_dir='.'):
    print(f"adding childjob {job} to {json_dir}")
    data=read_cache_data(json_dir)
    if "children" in data:
        old_paths=[os.path.relpath(p,json_dir) for p in data["children"]]
        data["children"]=list(set(old_paths).union({os.path.relpath(job,json_dir)}))
    else:
        data["children"]=[job]
    print(f"children={data['children']}, relpath={os.path.relpath(job,os.path.dirname(job))}")
    write_cache_data(data,json_dir)
def read_cache_data(dir):
    file=os.path.join(dir,settings.cache_file)
    if os.path.isfile(file):
        with open(file,"r") as fil:
            data=json.load(fil)
    else:
        data=dict()
    data['number_tries']=data.get('number_tries',0)
    return data
def determine_host(needed_gb,hosts):
    #random.shuffle(hosts)
    print(f'looking for host with {needed_gb}GB in {hosts}')
    starttime=datetime.now()
    wait=3#waittime in minutes
    while (datetime.now()-starttime<timedelta(days=1)):
        for host in hosts:
            cpu,mem=get_top_info(host)
            print(f"host {host} has {cpu}% free cpu capacity and {mem}GB free memory, needed={needed_gb}")
            if cpu>5 and mem>needed_gb:
                return host
            else:
                hosts.remove(host)
                hosts.append(host)
        #wait*=2
        print(f"waiting {wait} minutes to recheck host availability")
        time.sleep(wait*60)
    raise Exception(f"no good host available with {needed_gb}GB of memory")

def check_args(args):
    if "r" in args.action:
        if "commands" not in vars(args):
            raise Exception("specfiy command to run remotely")
        if "upload" not in vars(args):
            raise Exception("specfiy files to upload to run remotely")
        if "upload" not in vars(args):
            raise Exception("specfiy files to download once remotely")
def write_cache_data(data,dir):
    file=os.path.join(dir,settings.cache_file)
    with open(file,"w") as fil:
        json.dump(data,fil,indent=4)

def setup_and_run_job_remotely(args,jobdir=None):
    #function has two modes:
    # either jobdir is provided, then we are working our way down a determined structure and do not add a childjob
    #or not, and we add a childjob to os.getcwd()
    parentdir = os.getcwd() 
    #print(f"parentdir={parentdir},jobdir={jobdir},args.job={args.job}")
    if jobdir is None:
        jobdir=args.job
        if os.path.abspath(jobdir) != os.path.abspath(parentdir):
            add_childjob(jobdir,parentdir) 
    args=update_args(jobdir,args)
    if 'host' not in vars(args):
        args.host=determine_host(args.needed_gb,args.possible_hosts)

    if "r" in args.action:
        return run_job_remotely(jobdir,args)
def execute_commands_remotely(host,commands,jobdir,dir="~",wait=False,ignore_errors=False,simul=False):
    # delete entries older than a week
    logging_remote.logger.delete_old_entries(50)
    dircmd= f"mkdir -p {dir}\ncd {dir}\n"if dir != "~" else ""
    cmdlist="\n".join(commands)
    print(f"executing commands {commands} at {host}:{dir}")
    if simul:
        commandstring=f"ssh {host} '{dircmd}\necho {host} > host.txt\ncat <<END > run.sh\n{cmdlist}\nEND\nbash -i run.sh'"
        print(f"logging event:  starting calculation at {host}:{dir}")
        logging_remote.logger.log_event(f"starting calculation at {host}: {jobdir}")
    else:
        commandstring=f"ssh {host} \"{dircmd}{cmdlist}\""
    print(f"commandstring={commandstring}")
    proc=subprocess.Popen(commandstring, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if wait:
        out,err=proc.communicate()
        if len(err) and not ignore_errors:
            raise SSHError(err.decode("utf-8"))
        return out
    return proc

def upload_files(jobdir,host,remote_dir,upload_patterns):
    to_upload=[]
    for pat in upload_patterns:
        to_upload.extend(glob.glob(os.path.join(jobdir,pat)))
    print(f"uploading modell {[os.path.basename(p) for p in to_upload]}")
    execute_commands_remotely(host,[],jobdir,remote_dir,wait=True)#create directory
    proc=subprocess.Popen(f'scp {" ".join(to_upload)} {host}:{remote_dir}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
def run_job_remotely(jobdir,args):
    #print(f"running job remotely, args={args}")
    # write arguments
    print(f"writing args {args}")
    args.execute_time=datetime.now().strftime(settings.datetime_format)
    args.number_tries+=1
    args.status='executed'
    write_cache_data(vars(args),jobdir)
    upload_files(jobdir,args.host,args.remote_dir,args.upload)
    return execute_commands_remotely(args.host,args.commands,jobdir,args.remote_dir,simul=True)
def update_args(jobdir,args):
    #only to be called in final directory where simul has been launched from
    newdat=read_cache_data(jobdir)
    for arg in settings_not_to_update_locally:
        newdat.pop(arg,None)

    dict_view=vars(args)
    #existing args (dict_view) take precedence
    newargs= newdat| dict_view

    # add remote directory to arguments
    print(f"jobdir= {jobdir}")
    rel_path_local_anchor=os.path.relpath(jobdir,settings.local_anchor)
    if rel_path_local_anchor.startswith(".."):
        raise Exception(f"job {jobdir} is not under local anchor {settings.local_anchor}")
    if args.action!='c' or 'remote_dir' not in newdat:#do not overwrite remote_dir if making a download in case we have reshuffeled local dirs
        newargs["remote_dir"]=os.path.join(settings.remote_anchor,rel_path_local_anchor)
    print(f'updated_args={newargs}')
    newargs_namespace=argparse.Namespace(**newargs)
    check_args(newargs_namespace)
    return newargs_namespace
def is_calculation_done(jobdir,args):
    args=update_args(jobdir,args)
    #file_path=os.path.join(args.remote_dir,"start")
    if "host" not in vars(args):
        args.host=settings.default_host
    starttime_str = execute_commands_remotely(args.host,["stat -c '%Y' start"],jobdir,args.remote_dir,wait=True,ignore_errors=True).decode("utf-8")
    endtime_str = execute_commands_remotely(args.host,["stat -c '%Y' done"],jobdir,args.remote_dir,wait=True,ignore_errors=True).decode("utf-8")
    starttime=int(starttime_str) if len(starttime_str) else 0
    endtime=int(endtime_str) if len(endtime_str) else -1
    print(f"start={starttime},end={endtime}")
    return endtime>=starttime
def download_results(jobdir,args):
    print(f'checking for results of {jobdir}')
    if not os.path.isdir(jobdir):
        return ''
    cache_data=read_cache_data(jobdir)
    status=cache_data.get("status","")
    for i in range(1):#trivial loop that allows us to break out of it at any given time
        if status=='done' and not args.force:
            print(f"results in {jobdir} already downloaded")
            return 'already'
        if status=='aborted':
            return 'already_aborted'
        args=update_args(jobdir,args)
        if "host" not in vars(args):
            args.host=settings.default_host
        output=execute_commands_remotely(args.host,[f'[[ {args.remote_dir+"/start"} -nt {args.remote_dir+"/done"} ]] && echo yes || echo no'],jobdir,wait=True).decode()
        still_running=output.strip()=='yes'
        if still_running:
            cache_data['status']='running'
            break
        print(args.download)
        files_to_download = execute_commands_remotely(args.host,[f"ls {' '.join(args.download)}"],jobdir,args.remote_dir,wait=True,ignore_errors=True).decode("utf-8").strip().split("\n")
        if len(''.join(files_to_download))==0:
            print("found no files to download")
            cache_data['status']='aborted'
            break

        files_to_download=[f"{args.host}:{args.remote_dir}/"+fil for fil in files_to_download]
        print(f"downloading {[os.path.basename(f) for f in files_to_download]} to {jobdir}")
        
        out,err=subprocess.Popen(f'scp {" ".join(files_to_download)} {jobdir}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if len(err):
            raise SSHError(err.decode("utf-8"))
        cache_data['status']='done'
        cache_data['download_time']=datetime.now().strftime(settings.datetime_format)
    write_cache_data(cache_data,jobdir)
    return cache_data['status']
def traverse_dirs(jobdir,args):
    #returns true when calculation is done
    local_args=read_cache_data(jobdir)
    if "children" in local_args:
        children_dirs=[os.path.join(jobdir,child) for child in local_args["children"]]
        print(f"going to children directories:{children_dirs}")
        to_ret=[ret for child in children_dirs for ret in traverse_dirs(child,args)]
        return to_ret 
    else:
        #return lists because in upper branch we flatten lists over several possible children
        if len({"r","b"}.intersection(args.action)):
            procs=[setup_and_run_job_remotely(args,jobdir)]
            return procs
        elif "c" in args.action:
            code=download_results(jobdir,args)
            flag=code in {'already','just'}
            return [flag]
def main(args):
    print(f"\nREMOTECALC MAIN,args={args}")
    if "s" in args.action:
        logging_remote.logger.show_logs()
    if len({"r","b"}.intersection(args.action)) and args.job == "." and "children" not in read_cache_data('.'):
        setup_and_run_job_remotely(args)
    elif len({"r","b","c"}.intersection(args.action)) :
        procs=traverse_dirs(args.job,args)
if __name__=="__main__":
    args=parse_cmd_line()
    main(args)
