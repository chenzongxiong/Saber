package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;

import uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.utils.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.utils.Generator;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;

public class LinearRoadBenchmarkApp {

    public static void main (String [] args) throws InterruptedException, IOException {
        System.out.println("Usage: java class numberOfThreads filename lineToReads PRESET");

        LinearRoadBenchmarkQuery benchmarkQuery = null;
        int numberOfThreads = 1;
        int batchSize = 4 * 1048576;
        String executionMode = "cpu";
        int circularBufferSize = 128 * 1 * 1048576/2;
        int unboundedBufferSize = 4 * 1048576;
        int hashTableSize = 2*64*128;
        int partialWindows = 2;
        int slots = 1 * 128 * 1024*2;


        long totalLines = 1000000;
        String path = "/home/zongxiong/cardatapoints.out0";

        boolean usePAPI = false;
        String ICACHE_MISS_PRESET = "PAPI_L1_ICM,PAPI_L2_ICM";
        String DCACHE_MISS_PRESET = "PAPI_L1_DCM,PAPI_L2_DCM";
        String TCACHE_MISS_PRESET = "PAPI_L1_TCM,PAPI_L2_TCM,PAPI_L3_TCM";
        String TLB_PRESET = "PAPI_TLB_DM,PAPI_TLB_IM";
        String BRANCH_PRESET = "PAPI_BR_MSP,PAPI_BR_INS,PAPI_BR_TKN,PAPI_BR_NTK";
        String TOTAL_INSTR_CYCLE_PRESET = "PAPI_TOT_INS,PAPI_TOT_CYC";
        int TUPLES_TO_MEASURE = 40 * 1000000;
		/* Parse command line arguments */
		if (args.length == 1) {
			numberOfThreads = Integer.parseInt(args[0]);
        } else if (args.length == 2) {
            usePAPI = true;
            numberOfThreads = Integer.parseInt(args[0]);
            if (args[1].equals("ICACHE_MISS") || args[1].equals("icache_miss") ||
                args[1].equals("icache-miss")) {
                SystemConf.HW_PERF_COUNTERS = ICACHE_MISS_PRESET;
            }

            if (args[1].equals("DCACHE_MISS") || args[1].equals("dcache_miss") ||
                args[1].equals("dcache-miss")) {
                System.out.println("Hello world");
                SystemConf.HW_PERF_COUNTERS = DCACHE_MISS_PRESET;
            }

            if (args[1].equals("tcache-miss") || args[1].equals("tcache_miss") ||
                args[1].equals("TCACHE_MISS")) {
                SystemConf.HW_PERF_COUNTERS = TCACHE_MISS_PRESET;
            }

            if (args[1].equals("BRANCH") ||
                args[1].equals("branch")) {
                SystemConf.HW_PERF_COUNTERS = BRANCH_PRESET;
            }

            if (args[1].equals("TLB") || args[1].equals("tlb")) {
                SystemConf.HW_PERF_COUNTERS = TLB_PRESET;
            }

            if (args[1].equals("instr-cycle") || args[1].equals("INSTR_CYCLE")) {
                SystemConf.HW_PERF_COUNTERS = TOTAL_INSTR_CYCLE_PRESET;
            }
        } else {
            System.out.println("./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.LinearRoadBenchmarkApp <number-of-threads> <papi-preset>");
            System.out.println("papi-preset can be the following values: icache-miss, dcache-miss, tcache-miss, tlb, branch, instr-cycle");
            System.exit(0);
        }

        // Load test dataset into memory beforehand. Ensure data is loaded into memory
        BufferedReader bis = new BufferedReader(new FileReader(path));
        ArrayList<String> lines = new ArrayList<String>();
        for (long i = 0; i < totalLines; i ++) {
            lines.add(bis.readLine());
        }
        System.out.println("[DBG] Number of lines loaded into memory: " + lines.size());

        // Set SABER's configuration
        QueryConf queryConf = new QueryConf (batchSize);
        SystemConf.CIRCULAR_BUFFER_SIZE = circularBufferSize;
        SystemConf.UNBOUNDED_BUFFER_SIZE = 	unboundedBufferSize;
        SystemConf.HASH_TABLE_SIZE = hashTableSize;
        SystemConf.PARTIAL_WINDOWS = partialWindows;
        SystemConf.SLOTS = slots;
        SystemConf.SWITCH_THRESHOLD = 10;
        SystemConf.THROUGHPUT_MONITOR_INTERVAL = 1000L;
        SystemConf.SCHEDULING_POLICY = SystemConf.SchedulingPolicy.HLS;
        if (executionMode.toLowerCase().contains("cpu") || executionMode.toLowerCase().contains("hybrid"))
            SystemConf.CPU = true;
        if (executionMode.toLowerCase().contains("gpu") || executionMode.toLowerCase().contains("hybrid"))
            SystemConf.GPU = true;
        SystemConf.HYBRID = SystemConf.CPU && SystemConf.GPU;
        SystemConf.THREADS = numberOfThreads;
        SystemConf.LATENCY_ON = true;

        /* Initialize the Operators of the Benchmark */
        PAPIHardwareSampler [] papiSamplers = null;
        if (usePAPI) {
            papiSamplers = new PAPIHardwareSampler[SystemConf.THREADS + 2];
            for (int i = 0; i < papiSamplers.length; i ++) {
                papiSamplers[i] = new PAPIHardwareSampler(SystemConf.HW_PERF_COUNTERS);
            }
        }

        benchmarkQuery = new LinearRoadBenchmark (queryConf, true, papiSamplers);
        // zxchen: don't modify this parameter, inside circularbufferworker may use it, not sure.
        int bufferSize = 4 * 131072;
        int numberOfGeneratorThreads = 2;

        int coreToBind = 3; //numberOfThreads + 1;

        Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, coreToBind, lines);
        long timeLimit = System.currentTimeMillis() + 1000 * 1000;

        while (true) {
            if (timeLimit <= System.currentTimeMillis() ||
                (usePAPI && generator.totalGeneratedTuples > TUPLES_TO_MEASURE)) {

                System.out.println("Total Generated Tuples is: " + generator.totalGeneratedTuples);

                if (papiSamplers != null) {
                    try {
                        for (int i = 0; i < papiSamplers.length; i ++) {
                            papiSamplers[i].stopSampling("PAPI");
                        }

                        boolean once = true;
                        String [] keys = null;
                        long [] overall = null;
                        for (int i = 0; i < papiSamplers.length; i ++) {
                            HashMap<String, Long> result = papiSamplers[i].getResults();
                            if (once) {
                                keys = result.keySet().toArray(new String[result.keySet().size()]);
                                overall = new long[keys.length];

                                System.out.print("|  ");
                                for (int j = 0; j < keys.length; j ++) {
                                    System.out.print("| " + keys[j]);
                                    overall[j] = 0;
                                }
                                System.out.println("|");
                                once = false;
                            }

                            System.out.print("|  " + i);
                            for (int j = 0; j < keys.length; j ++) {
                                long value = result.get(keys[j]);
                                overall[j] += value;
                                System.out.print(" | " + value);
                            }
                            System.out.println("|");
                        }
                        System.out.print("| overall ");

                        for (int i = 0; i < overall.length; i ++) {
                            System.out.print(" |  " + overall[i]);
                        }
                        System.out.println("|");
                    } catch (Exception ex) {

                    }
                }
                if (SystemConf.LATENCY_ON) {
                    ((LinearRoadBenchmark) benchmarkQuery).stopLatencyMonitor();
                }


            	System.out.println("Terminating execution...");
                System.exit(0);
            }

            GeneratedBuffer b = generator.getNext();
            benchmarkQuery.getApplication().processData (b.getBuffer().array());
            b.unlock();
        }
    }
}
