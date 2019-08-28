#!/bin/bash

HOSTNAME=`hostname`
mkdir -p papi-result/$HOSTNAME

echo "Measure throughput 1"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 1  &> papi-result/$HOSTNAME/throughput-1.txt
echo "Measure throughput 2"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 2  &> papi-result/$HOSTNAME/throughput-2.txt
echo "Measure throughput 3"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 3  &> papi-result/$HOSTNAME/throughput-3.txt
echo "Measure throughput 4"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 4  &> papi-result/$HOSTNAME/throughput-4.txt


# echo "Measure throughput 5"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 5  &> papi-result/$HOSTNAME/throughput-5.txt
# echo "Measure throughput 6"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 6  &> papi-result/$HOSTNAME/throughput-6.txt
# echo "Measure throughput 7"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 7  &> papi-result/$HOSTNAME/throughput-7.txt
# echo "Measure throughput 8"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8  &> papi-result/$HOSTNAME/throughput-8.txt

# echo "Measure icache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 icache-miss &> papi-result/$HOSTNAME/icache-miss.txt
# echo "Measure dcache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 dcache-miss &> papi-result/$HOSTNAME/dcache-miss.txt
# echo "Measure tcache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tcache-miss &> papi-result/$HOSTNAME/tcache-miss.txt
# echo "Measure tlb"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tlb         &> papi-result/$HOSTNAME/tlb.txt
# echo "Measure branch"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 branch      &> papi-result/$HOSTNAME/branch.txt
# echo "Measure instr-cycle"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 instr-cycle &> papi-result/$HOSTNAME/instr-cycle.txt
