#!/bin/bash
cd "$(dirname $(dirname $0))"
task=clean_to_webdav
/usr/local/bin/docker-compose run --rm $task >> log/$task.out.log 2>> log/$task.err.log