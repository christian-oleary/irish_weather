#!/bin/bash

# Usage:
# ./weather.sh && find . -type f -empty -print -delete

set -B

declare -a levels=("h" "d" "m")
for i in {1000..9999}; do
    for l in "${levels[@]}"; do
        if [ -e $l'ly'$i'.zip' ]; then
            echo $l'ly'$i'.zip already exists'
        else
            echo 'Attemping download of '$l'ly'$i'.zip'
            curl -s -f 'https://cli.fusio.net/cli/climate_data/webdata/'$l'ly'$i'.zip' --output $l'ly'$i'.zip'
            sleep 5  # Just in case of rate limiting/automatic blacklisting
        fi
    done
done
