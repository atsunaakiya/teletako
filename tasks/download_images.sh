#!/bin/bash
cd "$(dirname $(dirname $0))"
task=image_crawler
time_limit=5m
timeout $time_limit /usr/local/bin/docker-compose run --rm $task >> log/$task.out.log 2>> log/$task.err.log