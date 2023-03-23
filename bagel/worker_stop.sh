#!/bin/sh

pid=$(ps aux | grep "./bin/worker $1" | grep -v grep | awk -F' ' '{print $2}')
if [ ! -z "${pid}" ]
then
  echo "Coord killing process ${pid}"
  kill "${pid}"
  echo "Coord killed process ${pid}"
fi
