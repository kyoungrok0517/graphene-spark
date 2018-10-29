#!/usr/bin/env bash
N_THREADS=${1}
sbt "runMain Main ./data/wikipedia-train/2 $N_THREADS"