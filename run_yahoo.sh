#!/bin/bash

./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 icache-miss &> papi-result/gpu4/icache-miss.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 dcache-miss &> papi-result/gpu4/dcache-miss.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tcache-miss &> papi-result/gpu4/tcache-miss.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tlb         &> papi-result/gpu4/tlb.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 branch      &> papi-result/gpu4/branch.txt
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 instr-cycle &> papi-result/gpu4/instr-cycle.txt
