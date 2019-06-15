# Rollbar scraper

This is a simple script to scrape Rollbars from your project at Rollbar.com
and put them into a Postgres DB for querying.

## Why
RQL is kind of slow :(

## Installation
```
$ virutalenv venv
$ pip install -r requirements.txt
```

## Usage
```bash
# Set the Rollbar project access token
$ export ROLLBAR_TOKEN=<yoyr project token here>

# Scrape
$ python scrape_rollbar.py <rollbar counter> <number of rollbars to scrape>

# Query
$ psql rollbars -c 'SELECT * FROM rollbars LIMIT 1;'
```
