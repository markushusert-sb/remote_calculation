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
    if "r" in args.action:
        if "commands" not in vars(args):
            raise Exception("specfiy command to run remotely")
        if "upload" not in vars(args):
            raise Exception("specfiy files to upload to run remotely")
        if "upload" not in vars(args):
            raise Exception("specfiy files to download once remotely")
def parse_cmd_line():
    parser = argparse.ArgumentParser(description='serves to launch simulations on a cluster and leave behind a trace permitting to download results automatically later')
    parser.add_argument('action',type=str,choices=["r","c","b"],nargs="+",help="action to do for specified jobs, r(un),b(uild) filestructure or c(echk) and download if job is done")
    #parser.add_argument('--group',"-g",type=str,help="which jobgrup to associate job with",default="0")
    parser.add_argument('--job',"-j",type=str,help="directory containing job to run, ignored if action is check",default=".")
    parser.add_argument('--commands','-c',type=str,nargs='+',help='commands to execute on remote host')
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
    print(f"adding childjob {job} to file {json_file}")
    data=read_cache_data(json_file)
    if "children" in data:
        old_paths=[os.path.relpath(p,os.path.dirname(json_file)) for p in data["children"]]
        data["children"]=list(set(old_paths).union({os.path.relpath(job,os.path.dirname(json_file))}))
    else:
        data["children"]=[job]
    print(f"children={data['children']}, relpath={os.path.relpath(job,os.path.dirname(job))}")
    with open(json_file,"w") as fil:
        json.dump(data,fil)
def read_cache_data(file):
    if os.path.isfile(file):
        with open(file,"r") as fil:
            return json.load(fil)
    else:
        return dict()

def setup_and_run_job_remotely(args,jobdir=None):
    #function has two modes:
    # either jobdir is provided, then we are worling our way down a determined structure and do not add a childjob
    #or not, and we add a childjob to os.getcwd()
    parentdir = os.getcwd() 
    #print(f"parentdir={parentdir},jobdir={jobdir},args.job={args.job}")
    if jobdir is None:
        jobdir=args.job
    if os.path.abspath(jobdir) != os.path.abspath(parentdir):
        add_childjob(jobdir,os.path.join(parentdir,settings.cache_file)) 
    update_args(jobdir,args)

    # write arguments
    to_write=os.path.join(jobdir,settings.cache_file)
    with open(to_write,"w") as fil:
        json.dump(vars(args),fil) 
    if "r" in args.action:
        return run_job_remotely(jobdir,args)
def execute_commands_remotely(host,dir,commands,wait=False,ignore_errors=False):
    cmdlist="\n".join(commands)
    print(f"executing commands {commands} at {host}:{dir}")
    commandstring=f"ssh {host} \"mkdir -p {dir} \ncd {dir}\n{cmdlist}\""
    proc=subprocess.Popen(commandstring, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if wait:
        out,err=proc.communicate()
        if len(err) and not ignore_errors:
            raise Exception(err.decode("utf-8"))
        return out
    return proc

def upload_files(jobdir,host,remote_dir,upload_patterns):
    to_upload=[]
    for pat in upload_patterns:
        to_upload.extend(glob.glob(os.path.join(jobdir,pat)))
    print(f"uploading modell to {to_upload}")
    execute_commands_remotely(host,remote_dir,[],wait=True)#create directory
    proc=subprocess.Popen(f'scp {" ".join(to_upload)} {host}:{remote_dir}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
def run_job_remotely(jobdir,args):
    #print(f"running job remotely, args={args}")
    upload_files(jobdir,args.host,args.remote_dir,args.upload)
    return execute_commands_remotely(args.host,args.remote_dir,args.commands)
def update_args(jobdir,args):
    #only to be called in final directory where simul has been launched from
    json_file=os.path.join(jobdir,settings.cache_file)
    newdat=read_cache_data(json_file)

    #print(f"update command line arguments by data, {newdat}, in {json_file}")
    dict_view=vars(args)
    dict_view.update( newdat | dict_view)

    # add remote directory to arguments
    rel_path_local_anchor=os.path.relpath(jobdir,settings.local_anchor)
    if rel_path_local_anchor.startswith(".."):
        raise Exception(f"job {jobdir} is not under local anchor {settings.local_anchor}")
    dict_view["remote_dir"]=os.path.join(settings.remote_anchor,rel_path_local_anchor)
    check_args(args)
def is_calculation_done(jobdir,args):
    update_args(jobdir,args)
    #file_path=os.path.join(args.remote_dir,"start")
    starttime_str = execute_commands_remotely(args.host,args.remote_dir,["stat -c \'%Y\' start"],wait=True,ignore_errors=True).decode("utf-8")
    endtime_str = execute_commands_remotely(args.host,args.remote_dir,["stat -c \'%Y\' done"],wait=True,ignore_errors=True).decode("utf-8")
    starttime=int(starttime_str) if len(starttime_str) else 0
    endtime=int(endtime_str) if len(endtime_str) else -1
    print(f"start={starttime},end={endtime}")
    return endtime>=starttime
def download_results(jobdir,args):
    files_to_download = execute_commands_remotely(args.host,args.remote_dir,[f"ls {' '.join(args.download)}"],wait=True).decode("utf-8").strip().split("\n")
    files_to_download=[f"{args.host}:{args.remote_dir}/"+fil for fil in files_to_download]
    print(f"downloading results to {jobdir}")
    
    out,err=subprocess.Popen(f'scp {" ".join(files_to_download)} {jobdir}', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    if len(err):
        raise Exception(err.decode("utf-8"))

def traverse_dirs(jobdir,args):
    #returns true when calculation is done
    local_args=read_cache_data(os.path.join(jobdir,settings.cache_file))
    #print(f"local_args={local_args}")
    if "children" in local_args:
        children_dirs=[os.path.join(jobdir,child) for child in local_args["children"]]
        print(f"going to children directories:{children_dirs}")
        to_ret=[ret for child in children_dirs for ret in traverse_dirs(child,args)]
        if "c" in args.action and all(to_ret):
            print(f"calculation under {jobdir} are done!")
        return to_ret 
    else:
        #return lists because in upper branch we flatten lists over several possible children
        if len({"r","b"}.intersection(args.action)):
            return [setup_and_run_job_remotely(args,jobdir)]
        elif "c" in args.action:
            flag=is_calculation_done(jobdir,args)
            if flag:
                print(f"calculation in {jobdir} is done!")
                download_results(jobdir,args)
            return [flag]
def main(args):
    if len({"r","b"}.intersection(args.action)) and args.job == "." and "children" not in read_cache_data(settings.cache_file):
        setup_and_run_job_remotely(args)
    else:
        procs=traverse_dirs(args.job,args)
        if "r" in args.action:
            print(f"waiting for {len(procs)} processes to finish")
            for proc in procs:
                #wait for all processes to finish
                proc.communicate()
            args.action='c'
            print(f"downloading results, starting in {args.job}")
            procs=traverse_dirs(args.job,args)

if __name__=="__main__":
    args=parse_cmd_line()
    main(args)
