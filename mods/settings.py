import os
datafile=os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),"list.tree")
cache_file=".remote_calc_data.json"
local_anchor="/home/markus/work/recherche/sujets"
remote_anchor="~/simulations"
download_done_file='download_done'
remote_logging_file="remote_calc_logs.log"
already_aborted_file="already_aborted.txt"
datetime_format="%m/%d/%Y, %H:%M"
