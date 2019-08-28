#!/bin/bash

HOSTNAME=`hostname`
mkdir -p papi-result/$HOSTNAME

# echo "Measure throughtput 5"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 5  &> papi-result/$HOSTNAME/throughtput-5.txt
# echo "Measure throughtput 6"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 6  &> papi-result/$HOSTNAME/throughtput-6.txt
# echo "Measure throughtput 7"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 7  &> papi-result/$HOSTNAME/throughtput-7.txt
# echo "Measure throughtput 8"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8  &> papi-result/$HOSTNAME/throughtput-8.txt

echo "Measure icache-miss"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 icache-miss &> papi-result/$HOSTNAME/icache-miss.txt
echo "Measure dcache-miss"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 dcache-miss &> papi-result/$HOSTNAME/dcache-miss.txt
echo "Measure tcache-miss"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tcache-miss &> papi-result/$HOSTNAME/tcache-miss.txt
echo "Measure tlb"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tlb         &> papi-result/$HOSTNAME/tlb.txt
echo "Measure branch"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 branch      &> papi-result/$HOSTNAME/branch.txt
echo "Measure instr-cycle"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 instr-cycle &> papi-result/$HOSTNAME/instr-cycle.txt
