#!/bin/bash

HOSTNAME=`hostname`
mkdir -p throughputs/$HOSTNAME

echo "Measure throughput 1"
numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 1  &> throughputs/$HOSTNAME/lrb/throughput-1.txt
echo "Measure throughput 2"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 2  &> throughputs/$HOSTNAME/lrb/throughput-2.txt
# echo "Measure throughput 3"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 3  &> throughputs/$HOSTNAME/lrb/throughput-3.txt
# echo "Measure throughput 4"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 4  &> throughputs/$HOSTNAME/lrb/throughput-4.txt


# echo "Measure throughput 5"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 5  &> throughputs/$HOSTNAME/lrb/throughput-5.txt
# echo "Measure throughput 6"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 6  &> throughputs/$HOSTNAME/lrb/throughput-6.txt
# echo "Measure throughput 7"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 7  &> throughputs/$HOSTNAME/lrb/throughput-7.txt
# echo "Measure throughput 8"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8  &> throughputs/$HOSTNAME/lrb/throughput-8.txt

# echo "Measure icache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8 icache-miss &> throughputs/$HOSTNAME/icache-miss.txt
# echo "Measure dcache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8 dcache-miss &> throughputs/$HOSTNAME/dcache-miss.txt
# echo "Measure tcache-miss"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8 tcache-miss &> throughputs/$HOSTNAME/tcache-miss.txt
# echo "Measure tlb"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8 tlb         &> throughputs/$HOSTNAME/tlb.txt
# echo "Measure branch"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8 branch      &> throughputs/$HOSTNAME/branch.txt
# echo "Measure instr-cycle"
# numactl --cpunodebind=1 --membind=1 ./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp 8 instr-cycle &> throughputs/$HOSTNAME/instr-cycle.txt
