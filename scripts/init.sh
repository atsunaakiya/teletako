#!/bin/bash
if [ ! -d venv ]
then
  echo "Creating venv"
  python -m venv venv
fi
source venv/bin/activate
pip install -r requirements.txt
if [ ! -f config.toml ]
then
  echo "Generating config.toml ..."
  cp config.sample.toml config.toml
fi
if [ ! -d cache ]
then
  echo "Creating ./cache"
  mkdir ./cache
fi
if [ ! -d data ]
then
  echo "Creating ./data"
  mkdir ./cache
fi
if [ ! -d log ]
then
  echo "Creating ./log"
  mkdir ./log
fi