#!/bin/bash -ex

set -o pipefail

IP=0.0.0.0
docker run -p 32379:32379 -p 32380:32380 pingcap/pd:v3.0.13 \
   -L debug \
   -initial-cluster pd=http://$IP:32380 \
   -peer-urls http://$IP:32380 \
   -name pd -data-dir /pd \
   -force-new-cluster \
   -advertise-client-urls http://$IP:32379 -client-urls http://$IP:32379