#!/usr/bin/bash

# Define the number of iterations
n=50

# Loop n times
while true; do
    # Execute your Bash command here
		download_all.py
    
    # Wait for 20 minutes before the next iteration
		timer=600
		echo waiting ${timer} seconds for next download
    sleep ${timer}
done

