#!/bin/sh

PROGRAM_VERSION=v1.2.3

mkdir .output
docker pull docker-registry.thunics.org:5000/bstack2proto3:${PROGRAM_VERSION}
docker run --rm -v $PWD/MessageProto:/home/work/bstack2proto3/MessageProto -v $PWD/wsn-data:/home/work/bstack2proto3/wsn-data -v $PWD/.output:/home/work/bstack2proto3/output docker-registry.thunics.org:5000/bstack2proto3:${PROGRAM_VERSION} /home/work/bstack2proto3/BStack2Proto3

