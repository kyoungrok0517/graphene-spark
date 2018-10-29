#!/usr/bin/env bash
N_THREADS=${1}
sbt "runMain Main ./data/wikipedia-train/4 $N_THREADS"