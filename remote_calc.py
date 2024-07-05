#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import timedelta
from datetime import datetime
import glob
import random
import time
import subprocess
import sys
import os
import json
import logging_remote
import logging
from datetime import datetime
from datetime import timedelta
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),"mods"))
try:
    import usersettings as settings
except ImportError:
    import settings
log=logging_remote.standart_logger(__name__)
log.setLevel(logging.INFO)
#log.setLevel(logging.DEBUG)
sys.path.pop(-1)
settings_not_to_update_locally={}
communication_blocked=datetime.min
timeout_default=30
limit_communication_blockage_minutes=0.5
limit_communication_blockage=timedelta(minutes=limit_communication_blockage_minutes)
def are_comms_blocked():
    return False #(might as well try to launch ssh commmand instead of assuming the worst)
    return datetime.now()-communication_blocked<limit_communication_blockage
class SSHError(Exception):
    def __init__(self, message):
        communication_blocked=datetime.now()
        super().__init__(message)
class SSHErrortemp(Exception):
    def __init__(self, message):
        communication_blocked=datetime.now()
        super().__init__(message)
parser = argparse.ArgumentParser(description='serves to launch simulations on a cluster and leave behind a trace permitting to download results automatically later')
parser.add_argument('action',type=str,choices=["r","c","b","s"],nargs="+",help="action to do for specified jobs, r(un),b(uild) filestructure or c(echk) and download if job is done, s(top) job")
parser.add_argument('--job',"-j",type=os.path.abspath,help="directory containing job to run, ignored if action is check",default=".")
parser.add_argument('--age',"-a",type=float,help="only considered if no job specified, instead acts on all jobs younger then <age> hours",default=0.0)
parser.add_argument('--commands','-c',type=str,nargs='+',help='commands to execute on remote host')
parser.add_argument('--host', type=str,help='host to run on',default=None)
parser.add_argument('--upload','-u', type=str,nargs='+',help='files to upload, will be globbed in local dir',default=["*.mesh","*.edp"])
parser.add_argument('--download','-d', type=str,nargs='+',help='filepatterns to download')
parser.add_argument('--needed_gb', type=int,help='estimated gb needed to carry cout calulation. feel free to add safety factor. script will search inside of args.possible_hosts for host with sufficient memory',default=4)
parser.add_argument('--possible_hosts', type=str,nargs='+',help='lists of all admissible hosts for job. by default takes all of the machines',default=["keket","moreau","hermes","boch","hera","hades","xeller","poseidon"])
parser.add_argument('--force','-f', action='store_true',help='force download of files even if already downloaded')
parser.add_argument('--wait','-w', action="store_true",help='wait for calculation to be done?')
def parse_cmd_line():
    args = parser.parse_args()

    for key,val in list(vars(args).items()):
        if val is None:
            vars(args).pop(key)

    return args
def find_results_in_dir(dir,pattern="'Chomo*.csv'"):
    proc=subprocess.run(f"find {dir} -name {pattern}",shell=True,capture_output=True)
    return proc.stdout.decode().strip().split("\n")

def get_top_info(host):
    try:
        output,err=execute_commands_remotely(host,["top -b -n 1"],'',wait=True,timeout=timeout_default)
        output=output.decode("utf-8")
    except (subprocess.TimeoutExpired,SSHErrortemp):
        return 0,0
    lines=output.split("\n")
    if len(''.join(lines))==0:
        return 0,0 #could not connect to host
    free_memory=(int(lines[3][26:32])+int(lines[3][56:62]))/1000.
    cpu=int(lines[2][36:38])
    return cpu,free_memory
def add_childjob(job,json_dir='.'):
    log.info(f"adding childjob {job} to {json_dir}")
    data=read_cache_data(json_dir)
    if "children" in data:
        old_paths=[os.path.relpath(p,json_dir) for p in data["children"]]
        data["children"]=list(set(old_paths).union({os.path.relpath(job,json_dir)}))
    else:
        data["children"]=[job]
    log.info(f"children={data['children']}, relpath={os.path.relpath(job,os.path.dirname(job))}")
    write_cache_data(data,json_dir)
def read_cache_data(dir):
    file=os.path.join(dir,settings.cache_file)
    if os.path.isfile(file):
        with open(file,"r") as fil:
            data=json.load(fil)
            log.debug(data)
    else:
        data=dict()
    data['number_tries']=data.get('number_tries',0)
    return data
def determine_host(needed_gb,hosts,max_number_turns=None):
    #random.shuffle(hosts)
    log.info(f'looking for host with {needed_gb}GB in {hosts}')
    starttime=datetime.now()
    wait=1#waittime in minutes
    counter=0
    while (datetime.now()-starttime<timedelta(days=1)):
        counter+=1
        for host in hosts:
            try:
                cpu,mem=get_top_info(host)
            except SSHErrortemp: 
                continue
            log.info(f"{datetime.now()}: host {host} has {cpu}% free cpu capacity and {mem}GB free memory, needed={needed_gb}")
            if cpu>5 and mem>needed_gb:
                return host
            else:
                hosts.remove(host)
                hosts.append(host)
        #wait*=2
        if max_number_turns is not None and counter>=max_number_turns:
            break
        log.info(f"waiting {wait} minutes to recheck host availability")
        time.sleep(wait*60)
    else:
        raise Exception(f"no good host available with {needed_gb}GB of memory")
    return None

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
def get_calculations_older_than_x_hours(x,rewrite=False):
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.remote_logging_file),"r") as fil:
        lines=fil.readlines()
    lines_to_keep=[]
    old_lines=[]
    to_check=[]
    done_paths=set()
    for line in lines:
        components=line.split(" ")
        date=components[0]
        hour=components[1]
        launchtime=datetime.strptime(f"{date}",'%Y-%m-%d')
        path=components[-1].strip()
        host=components[-2].replace(":",'')
        if not os.path.isdir(path) or path in done_paths:
            continue 
        now = datetime.now() 
        diff=now-launchtime
        if diff.seconds <=x*3600:
            done_paths.add(path)
            to_check.append(path)
            lines_to_keep.append(line)
        else:
            old_lines.append(line)
    if rewrite:
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.remote_logging_file),"w") as fil:
            fil.writelines(lines_to_keep)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),settings.remote_logging_file)+".old","a+") as fil:
            fil.writelines(old_lines)
    return to_check
def create_regular_download_file():
    if "regular_commands_dir" in os.environ:
        with open(os.path.join(os.environ['regular_commands_dir'],'download_all.py'),"w") as fil:
            fil.write("6")
def setup_and_run_job_remotely(args,jobdir=None):
    #function has two modes:
    # either jobdir is provided, then we are working our way down a determined structure and do not add a childjob
    #or not, and we add a childjob to os.getcwd()
    parentdir = os.getcwd() 
    #log.info(f"parentdir={parentdir},jobdir={jobdir},args.job={args.job}")
    if jobdir is None:
        jobdir=args.job
        if os.path.abspath(jobdir) != os.path.abspath(parentdir):
            add_childjob(jobdir,parentdir) 
    args=update_args(jobdir,args)
    if 'r' in args.action and args.host=='':
        args.host=determine_host(args.needed_gb,args.possible_hosts)
    create_regular_download_file()

    #creating preliminairy cache file in case launching of calculation fails
    args.status='submitted'
    write_cache_data(vars(args),jobdir)

    if "r" in args.action:
        return run_job_remotely(jobdir,args)
    else:
        logging_remote.logger.log_event(f"submitting calculation for {vars(args).get('host','None')}: {os.path.abspath(jobdir)}")

def execute_commands_remotely(host,commands,jobdir,dir="~",wait=True,ignore_errors=False,simul=False,timeout=None):
    # delete entries older than a week
    logging_remote.logger.delete_old_entries(50)
    dircmd= f"mkdir -p {dir}\ncd {dir}\n"if dir != "~" else ""
    log.info(f'dir={dir},dircmd={dircmd}')
    cmdlist="\n".join(commands)
    log.info(f"executing commands {commands} at {host}:{dir}")
    if simul:
        commandstring=f"ssh {host} '{dircmd}\necho {host} > host.txt\ncat <<END > run.sh\n{cmdlist}\nEND\nbash -i run.sh'"
        logging_remote.logger.log_event(f"starting calculation at {host}: {os.path.abspath(jobdir)}")
    else:
        commandstring=f"ssh {host} \"{dircmd}{cmdlist}\""
    log.info(f"commandstring={commandstring}")
    proc=subprocess.Popen(commandstring, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.debug(f"wait={wait},timeout={timeout}")
    if wait:
        try:
            out,err=proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
          proc.kill() 
          log.info('aborting process due to timeout')
          raise
        if len(err) and not ignore_errors:
            text=err.decode("utf-8")
            if 'Temporary failure' in text:
                raise SSHErrortemp(err.decode("utf-8"))
            else:
                log.info(f'SSHerror:{text}')
                raise SSHError(err.decode("utf-8"))
        return out,err
    return proc

def upload_files(jobdir,host,remote_dir,upload_patterns):
    to_upload=[]
    for pat in upload_patterns:
        to_upload.extend(glob.glob(os.path.join(jobdir,pat)))
    log.info(f"uploading modell {[os.path.basename(p) for p in to_upload]}")
    execute_commands_remotely(host,[],jobdir,remote_dir,wait=True,timeout=timeout_default)#create directory
    proc=subprocess.Popen(f'scp {" ".join(to_upload)} {host}:{remote_dir}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate(timeout=2*timeout_default)
def run_job_remotely(jobdir,args):
    #log.info(f"running job remotely, args={args}")
    # write arguments
    log.info(f"writing args {args}")
    args.execute_time=datetime.now().strftime(settings.datetime_format)
    args.number_tries+=1
    args.status='executed'
    upload_files(jobdir,args.host,args.remote_dir,args.upload)
    retval=execute_commands_remotely(args.host,args.commands,jobdir,args.remote_dir,simul=True,wait=False)
    write_cache_data(vars(args),jobdir)
    return retval
def update_args(jobdir,args):
    #only to be called in final directory where simul has been launched from
    newdat=read_cache_data(jobdir)
    for arg in settings_not_to_update_locally:
        newdat.pop(arg,None)

    dict_view=vars(args)
    given_args={key:val for key,val in dict_view.items() if val is not None}
    log.info(given_args)
    #existing args (given_args) take precedence
    newargs=dict_view| newdat| given_args

    # add remote directory to arguments
    log.info(f"jobdir= {jobdir}")
    rel_path_local_anchor=os.path.relpath(jobdir,settings.local_anchor)
    if 'c' not in args.action or 'remote_dir' not in newdat:#do not overwrite remote_dir if making a download in case we have reshuffeled local dirs
        if rel_path_local_anchor.startswith(".."):
            raise Exception(f"job {jobdir} is not under local anchor {settings.local_anchor}")
        newargs["remote_dir"]=os.path.join(settings.remote_anchor,rel_path_local_anchor)
    log.debug(f'updated_args={newargs}')
    newargs_namespace=argparse.Namespace(**newargs)
    check_args(newargs_namespace)
    return newargs_namespace
def stop_process(jobdir,args):
    args=update_args(jobdir,args)
    status=args.status
    if status in {'running','executed'} or True:
        if 'host' in vars(args):
            out,err=execute_commands_remotely(args.host,[r"ls pid_* | xargs awk '{print \$1}' | xargs kill -1 "],'',args.remote_dir,wait=True)
            log.info(out)
            pass
def download_results(jobdir,args):
    log.info(f'checking for results of {jobdir}')
    if not os.path.isdir(jobdir):
        return ''
    cache_data=read_cache_data(jobdir)
    retval=download_results_inner(cache_data,args,jobdir)
    if not args.force:
        write_cache_data(cache_data,jobdir)
    return retval
def download_results_inner(cache_data,args,jobdir):
    status=cache_data.get("status","")
    if status=='done' and not args.force:
        log.info(f"results in {jobdir} already downloaded")
        return 'already'
    if status=='submitted':
        return cache_data['status']
    if status=='aborted' and not args.force:
        return 'already_aborted'
    args=update_args(jobdir,args)
    while True:
        host=random.choice(args.possible_hosts)
        try: 
            output,err=execute_commands_remotely(cache_data['host'],[r"ls pid_* | xargs awk '{print \$1}' | xargs ps -p"],'',args.remote_dir,wait=True,timeout=timeout_default)
            output=output.decode().strip()
            still_running=len(output.split('\n'))>1
            log.debug(f'output of running check: {output}, still running={still_running}')
            if still_running:
                log.info('calculation is still running')
                cache_data['status']='running'
                return cache_data['status']
            output,error= execute_commands_remotely(host,[f"ls {' '.join(args.download)}"],jobdir,args.remote_dir,wait=True,ignore_errors=True,timeout=timeout_default)
            files_to_download = output.decode("utf-8").strip().split("\n")
            log.debug(f"files_to_download={files_to_download}")
            if len(''.join(files_to_download))==0:
                log.info("found no files to download")
                cache_data['status']='aborted'
                return cache_data['status']

            files_to_download=[f"{host}:{args.remote_dir}/"+fil for fil in files_to_download]
            log.info(f"downloading {[os.path.basename(f) for f in files_to_download]} via {host} to {jobdir}")
            downloadstr=f'scp {" ".join(files_to_download)} {jobdir}'
            log.debug(f"downloadstr={downloadstr}") 
            out,err=subprocess.Popen(downloadstr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate(timeout=timeout_default)
            if len(err):
                raise SSHErrortemp(err.decode("utf-8"))
            if len(error)==0:
                cache_data['status']='done'
                cache_data['download_time']=datetime.now().strftime(settings.datetime_format)
            else:
                log.info('not all results have been generated')
                cache_data['status']='aborted'

            return cache_data['status']
        except subprocess.TimeoutExpired:
            args.possible_hosts.pop(args.possible_hosts.index(host))
            continue
        except SSHErrortemp:
            args.possible_hosts.pop(args.possible_hosts.index(host))
            continue
def traverse_dirs(jobdir,args):
    #returns true when calculation is done
    local_args=read_cache_data(jobdir)
    if "children" in local_args:
        return #no longer want children mechanic for now
        children_dirs=[os.path.join(jobdir,child) for child in local_args["children"]]
        log.info(f"going to children directories:{children_dirs}")
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
        elif "s" in args.action:
            stop_process(jobdir,args)
            return []
def main(args):
    log.info(f"\nREMOTECALC MAIN,args={args}")
    if os.path.samefile(args.job,os.getcwd())  and args.age !=0.0:
        jobs=get_calculations_older_than_x_hours(args.age)
        log.debug(jobs)
        for job in jobs:
            traverse_dirs(job,args)
    else:
        procs=traverse_dirs(args.job,args)
if __name__=="__main__":
    args=parse_cmd_line()
    main(args)
