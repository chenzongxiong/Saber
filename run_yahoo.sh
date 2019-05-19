#!/bin/bash

./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 icache-miss &> target/tmp/icache-miss.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 dcache-miss &> target/tmp/dcache-miss.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tcache-miss &> target/tmp/tcache-miss.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tlb         &> target/tmp/tlb.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 branch      &> target/tmp/branch.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 instr-cycle &> target/tmp/instr-cycle.txt
