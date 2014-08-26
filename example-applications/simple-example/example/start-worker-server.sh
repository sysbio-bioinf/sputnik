#!/bin/sh

cd server-payload && echo "Starting the server ..." && ./start-localhost-server.sh && cd ../worker-payload && echo "Starting the worker ..." && ./start-localhost-worker.sh
