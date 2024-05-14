#!/usr/bin/bash

# Define the number of iterations
n=50

# Loop n times
for ((i=1; i<=$n; i++)); do
    # Execute your Bash command here
		download_all.py
    
    # Wait for 20 minutes before the next iteration
    sleep 1200
done

