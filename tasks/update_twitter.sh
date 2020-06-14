#!/bin/bash
cd "$(dirname $(dirname $0))"
task=twitter_crawler
time_limit=2m
timeout $time_limit /usr/local/bin/docker-compose run --rm $task >> log/$task.out.log 2>> log/$task.err.log