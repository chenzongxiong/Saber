package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.GeneratedBuffer;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.Generator;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

public class YahooBenchmarkApp {
	public static final String usage = "usage: YahooBenchmarkApp with in-memory generation";

	public static void main (String [] args) throws InterruptedException {

		YahooBenchmarkQuery benchmarkQuery = null;
		int numberOfThreads = 1;
		int batchSize = 4 * 1048576;
		String executionMode = "cpu";
		int circularBufferSize = 128 * 1 * 1048576/2;
		int unboundedBufferSize = 4 * 1048576;
		int hashTableSize = 2*64*128;
		int partialWindows = 2;
		int slots = 1 * 128 * 1024*2;

		boolean isV2 = false; // change the tuple size to half if set true

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
            System.out.println("./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp <number-of-threads> <papi-preset>");
            System.out.println("papi-preset can be the following values: icache-miss, dcache-miss, tcache-miss, tlb, branch, instr-cycle");
            System.exit(0);
        }

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
		// SystemConf.LATENCY_ON = false;
        SystemConf.LATENCY_ON = true;
		/* Initialize the Operators of the Benchmark */
        PAPIHardwareSampler [] papiSamplers = null;
        if (usePAPI) {
            papiSamplers = new PAPIHardwareSampler[SystemConf.THREADS + 2];
            for (int i = 0; i < papiSamplers.length; i ++) {
                papiSamplers[i] = new PAPIHardwareSampler(SystemConf.HW_PERF_COUNTERS);
            }
        }

        benchmarkQuery = new YahooBenchmark (queryConf, true, null, isV2, papiSamplers);
		/* Generate input stream */
		int numberOfGeneratorThreads = 2;
		int adsPerCampaign = ((YahooBenchmark) benchmarkQuery).getAdsPerCampaign();
		long[][] ads = ((YahooBenchmark) benchmarkQuery).getAds();


		int bufferSize = 4 * 131072;
		int coreToBind = 3; //numberOfThreads + 1;


		Generator generator = new Generator (bufferSize, numberOfGeneratorThreads, adsPerCampaign, ads, coreToBind, isV2);
        long timeLimit = System.currentTimeMillis() + 1 * 10000;

		while (true) {
			if (timeLimit <= System.currentTimeMillis() ||
                (usePAPI && generator.totalGeneratedTuples >= TUPLES_TO_MEASURE)) {
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
                    ((YahooBenchmark) benchmarkQuery).stopLatencyMonitor();
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
