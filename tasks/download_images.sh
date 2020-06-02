#!/bin/bash
cd "$(dirname $(dirname $0))"
task=image_crawler
/usr/local/bin/docker-compose run --rm $task >> log/$task.out.log 2>> log/$task.err.log