#!/bin/bash

if [ -z "$1" ]; then echo "You must specify a scraper"; exit 1; fi

scraper=`basename "$1" | sed 's/\\.py$//'`
db="db/$scraper.sqlite"
script="scrapers/$scraper.py"

# Pre-flight checks
if [ ! -f "$script" ]; then echo "Scraper $script could not be found"; exit 1; fi
if [ ! -f "$db" ]; then echo "Database $db could not be found"; exit 1; fi

# Script args
args=
if [ "$2" == "test" ]; then args="$args test"; fi

# Run the script
export SCRAPERWIKI_DATABASE_NAME="sqlite:///$db"
python "$script" $args
