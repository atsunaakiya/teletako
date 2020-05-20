#!/bin/bash
cd "$(dirname $(dirname $0))"
task=telegram_poster
/usr/local/bin/docker-compose run $task >> log/$task.out.log 2>> log/$task.err.log