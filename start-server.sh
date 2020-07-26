#!/bin/bash -ex

set -o pipefail

PDIP=127.0.0.1
STOREADDR=127.0.0.1:$1
./bin/tinykv-server -loglevel debug -scheduler http://$PDIP:32379 -addr $STOREADDR -path /tmp/s-$1
