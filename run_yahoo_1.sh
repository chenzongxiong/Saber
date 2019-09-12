#!/bin/bash

HOSTNAME=`hostname`
mkdir -p throughputs/$HOSTNAME

# echo "Measure throughput 1"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 1  &> throughputs/$HOSTNAME/throughput-1.txt
# echo "Measure throughput 2"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 2  &> throughputs/$HOSTNAME/throughput-2.txt
# echo "Measure throughput 3"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 3  &> throughputs/$HOSTNAME/throughput-3.txt
# echo "Measure throughput 4"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 4  &> throughputs/$HOSTNAME/throughput-4.txt

# echo "Measure throughput 5"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 5  &> throughputs/$HOSTNAME/throughput-5.txt
# echo "Measure throughput 6"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 6  &> throughputs/$HOSTNAME/throughput-6.txt
# echo "Measure throughput 7"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 7  &> throughputs/$HOSTNAME/throughput-7.txt
# echo "Measure throughput 8"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8  &> throughputs/$HOSTNAME/throughput-8.txt

# echo "Measure icache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 icache-miss &> throughputs/$HOSTNAME/icache-miss.txt
# echo "Measure dcache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 dcache-miss &> throughputs/$HOSTNAME/dcache-miss.txt
# echo "Measure tcache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tcache-miss &> throughputs/$HOSTNAME/tcache-miss.txt
# echo "Measure tlb"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 tlb         &> throughputs/$HOSTNAME/tlb.txt
# echo "Measure branch"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 branch      &> throughputs/$HOSTNAME/branch.txt
# echo "Measure instr-cycle"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8 instr-cycle &> throughputs/$HOSTNAME/instr-cycle.txt

echo "Measure throughput 1"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 1  &> throughputs/$HOSTNAME/throughput-1.txt
echo "Measure throughput 2"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 2  &> throughputs/$HOSTNAME/throughput-2.txt
echo "Measure throughput 4"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 4  &> throughputs/$HOSTNAME/throughput-4.txt
echo "Measure throughput 8"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 8  &> throughputs/$HOSTNAME/throughput-8.txt
echo "Measure throughput 12"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 12  &> throughputs/$HOSTNAME/throughput-12.txt
echo "Measure throughput 18"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 18  &> throughputs/$HOSTNAME/throughput-18.txt
echo "Measure throughput 24"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 24  &> throughputs/$HOSTNAME/throughput-24.txt
echo "Measure throughput 36"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 36  &> throughputs/$HOSTNAME/throughput-36.txt
echo "Measure throughput 42"
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp 42  &> throughputs/$HOSTNAME/throughput-42.txt
