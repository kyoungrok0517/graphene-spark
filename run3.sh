#!/usr/bin/env bash
N_THREADS=${1}
sbt "runMain Main ./data/wikipedia-train/3 $N_THREADS"