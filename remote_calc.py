#!/usr/bin/env python3
import argparse
import glob
import subprocess
import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),"mods"))
import settings
sys.path.pop(-1)
from anytree import Node, RenderTree
def check_args(args):
    print(args)
    if "r" in args.action:
        if "command" not in vars(args):
            raise Exception("specfiy command to run remotely")
        if "upload" not in vars(args):
            raise Exception("specfiy files to upload to run remotely")
        if "upload" not in vars(args):
            raise Exception("specfiy files to download once remotely")
def parse_cmd_line():
    parser = argparse.ArgumentParser(description='serves to launch simulations on a cluster and leave behind a trace permitting to download results automatically later')
    parser.add_argument('action',type=str,choices=["r","c"],nargs="+",help="action to do for specified jobs, r(un) or c(echk) and download if job is done")
    #parser.add_argument('--group',"-g",type=str,help="which jobgrup to associate job with",default="0")
    parser.add_argument('--job',"-j",type=str,help="directory containing job to run, ignored if action is check",default=".")
    parser.add_argument('--command','-c',type=str,help='command to execute on remote host')
    parser.add_argument('--host', type=str,help='host to run on',default='hades')
    parser.add_argument('--upload','-u', type=str,nargs='+',help='files to upload, will be globbed in local dir',default=["*.mesh","*.edp"])
    parser.add_argument('--download','-d', type=str,nargs='+',help='filepatterns to download')
    args = parser.parse_args()

    for key,val in list(vars(args).items()):
        if val is None:
            vars(args).pop(key)

    return args
def setup_job_dir(cache_dir):
    pass     
def add_childjob(job,json_file=settings.cache_file):
    data=read_cache_data(json_file)
    if "children" in data:
        data["children"]=list(set(data["children"]).union({job}))
    else:
        data["children"]=[job]
    with open(json_file,"w") as fil:
        json.dump(data,fil)
def read_cache_data(file):
    if os.path.isfile(file):
        with open(file,"r") as fil:
            return json.load(fil)
    else:
        print(f"returning empty dict for {file}")
        return dict()

def setup_and_run_job_remotely(args):
    jobdir=args.job
    update_args(jobdir,args)
    if os.path.abspath(jobdir) != os.path.abspath(os.getcwd()):
        add_childjob(jobdir) 

    # write arguments
    to_write=os.path.join(jobdir,settings.cache_file)
    with open(to_write,"w") as fil:
        json.dump(vars(args),fil) 
    run_job_remotely(jobdir,args)
def execute_commands_remotely(host,dir,commands,wait=False):
    cmdlist="\n".join(commands)
    commandstring=f"ssh {host} \"mkdir -p {dir} \ncd {dir}\n{cmdlist}\""
    proc=subprocess.Popen(commandstring, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if wait:
        out,err=proc.communicate()
        return out
def upload_files(jobdir,host,remote_dir,upload_patterns):
    to_upload=[]
    for pat in upload_patterns:
        to_upload.extend(glob.glob(os.path.join(jobdir,pat)))
    print(f"uploading modell to {to_upload}")
    os.system(f'scp {" ".join(to_upload)} {host}:{remote_dir}')
def run_job_remotely(jobdir,args):
    print(f"running job remotely, args={args}")
    upload_files(jobdir,args.host,args.remote_dir,args.upload)
    #execute_commands_remotely(args.host,args.remote_dir,[args.command])
def update_args(jobdir,args):
    #only to be called in final directory where simul has been launched from
    json_file=os.path.join(jobdir,settings.cache_file)

    print(f"update command line arguments by data in {json_file}")
    dict_view=vars(args)
    dict_view.update(read_cache_data(json_file) | dict_view)

    # add remote directory to arguments
    rel_path_local_anchor=os.path.relpath(jobdir,settings.local_anchor)
    if rel_path_local_anchor.startswith(".."):
        raise Exception(f"job {jobdir} is not under local anchor {settings.local_anchor}")
    dict_view["remote_dir"]=os.path.join(settings.remote_anchor,rel_path_local_anchor)
    check_args(args)
def is_calculation_done(jobdir,args):
    update_args(jobdir,args)
    #file_path=os.path.join(args.remote_dir,"start")
    starttime_byte_str = execute_commands_remotely(args.host,args.remote_dir,["stat -c \'%Y\' start"],wait=True)#os.system(f'ssh {args.host} "').read().strip() 
    endtime_byte_str = execute_commands_remotely(args.host,args.remote_dir,["stat -c \'%Y\' done"],wait=True)#os.system(f'ssh {args.host} "stat -c \'%Y\' {file_path}"').read().strip() 
    print(endtime_byte_str)
    starttime=int(starttime_byte_str.decode("utf-8")) if len(starttime_byte_str) else 0
    endtime=int(endtime_byte_str.decode("utf-8")) if len(endtime_byte_str) else -1
    return endtime>=starttime
def download_results(jobdir,args):
    files_to_download = execute_commands_remotely(args.host,args.remote_dir,[f"ls {' '.join(args.download)}"],wait=True).decode("utf-8").strip().split("\n")
    files_to_download=[f"{args.host}:{args.remote_dir}/"+fil for fil in files_to_download]
    print(f"downloading results to {jobdir}")
    os.system(f'scp {" ".join(files_to_download)} {jobdir}')

def check_calulation(jobdir,args):
    #returns true when calculation is done
    local_args=read_cache_data(os.path.join(jobdir,settings.cache_file))
    print(f"local_args={local_args}")
    if "children" in local_args:
        children_dirs=[os.path.join(jobdir,child) for child in local_args["children"]]
        print(f"children={children_dirs}")
        doneflag=all([check_calulation(child,args) for child in children_dirs])
        if doneflag:
            print(f"calculations in {jobdir} are done!")
        return doneflag
    else:
        doneflag=is_calculation_done(jobdir,args)
        if doneflag:
            print(f"calculation in {jobdir} is done!")
            download_results(jobdir,args)
        return doneflag
def main():
    args=parse_cmd_line()
    if "r" in args.action:
        setup_and_run_job_remotely(args)#(args.command,args.job,args.host,args.upload)
    elif "c" in args.action:
        check_calulation(args.job,args)

if __name__=="__main__":
    main()
