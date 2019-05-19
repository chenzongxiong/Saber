/*
 * This is generated file. Do not edit.
 * To change this file, see the jni/genconst.c script and run
 * make afterwards.
 */

package papi;
//package uk.ac.imperial.lsds.saber.hardware.papi;
public class Constants {
    public static final int PAPI_VER_CURRENT = 84213760;
    /* Error codes */
	public static final int PAPI_OK = 0;
	public static final int PAPI_EINVAL = -1;

	/* Mixed events */
	public static final int CPU_CLK_UNHALTED_THREAD_P = 1073741871;
	public static final String CPU_CLK_UNHALTED_THREAD_P_name = "CPU_CLK_UNHALTED:THREAD_P";
	public static final int IDQ_UOPS_NOT_DELIVERED_CORE = 1073741872;
	public static final String IDQ_UOPS_NOT_DELIVERED_CORE_name = "IDQ_UOPS_NOT_DELIVERED:CORE";
	public static final int UOPS_ISSUED = 1073741873;
	public static final String UOPS_ISSUED_name = "UOPS_ISSUED";
	public static final int UNHALTED_CORE_CYCLES = 1073741874;
	public static final String UNHALTED_CORE_CYCLES_name = "UNHALTED_CORE_CYCLES";
	public static final int RESOURCE_STALLS_ANY = 1073741849;
	public static final String RESOURCE_STALLS_ANY_name = "RESOURCE_STALLS:ANY";
	public static final int INT_MISC_RECOVERY_CYCLES = 1073741875;
	public static final String INT_MISC_RECOVERY_CYCLES_name = "INT_MISC:RECOVERY_CYCLES";
	public static final int UOPS_RETIRED_RETIRE_SLOTS = 1073741876;
	public static final String UOPS_RETIRED_RETIRE_SLOTS_name = "UOPS_RETIRED:RETIRE_SLOTS";
	public static final int CYCLE_ACTIVITY_CYCLES_LDM_PENDING = 1073741877;
	public static final String CYCLE_ACTIVITY_CYCLES_LDM_PENDING_name = "CYCLE_ACTIVITY:CYCLES_LDM_PENDING";
	public static final int UOPS_EXECUTED_CORE_CYCLES_NONE = 1073741878;
	public static final String UOPS_EXECUTED_CORE_CYCLES_NONE_name = "UOPS_EXECUTED:CORE_CYCLES_NONE";
	public static final int UOPS_EXECUTED_CORE_c_1 = 1073741879;
	public static final String UOPS_EXECUTED_CORE_c_1_name = "UOPS_EXECUTED:CORE:c=1";
	public static final int UOPS_EXECUTED_CORE_c_2 = 1073741880;
	public static final String UOPS_EXECUTED_CORE_c_2_name = "UOPS_EXECUTED:CORE:c=2";
	public static final int CYCLE_ACTIVITY_STALLS_L1D_MISS = 1073741881;
	public static final String CYCLE_ACTIVITY_STALLS_L1D_MISS_name = "CYCLE_ACTIVITY:STALLS_L1D_MISS";
	public static final int CYCLE_ACTIVITY_STALLS_L2_MISS = 1073741882;
	public static final String CYCLE_ACTIVITY_STALLS_L2_MISS_name = "CYCLE_ACTIVITY:STALLS_L2_MISS";
	public static final int perf_L1_ICACHE_LOAD_MISSES = 1073741883;
	public static final String perf_L1_ICACHE_LOAD_MISSES_name = "perf::L1-ICACHE-LOAD-MISSES";
	public static final int CYCLE_ACTIVITY_STALLS_MEM_ANY = 1073741885;
	public static final String CYCLE_ACTIVITY_STALLS_MEM_ANY_name = "CYCLE_ACTIVITY:STALLS_MEM_ANY";
	public static final int ICACHE_64B_IFTAG_STALL = 1073741886;
	public static final String ICACHE_64B_IFTAG_STALL_name = "ICACHE_64B:IFTAG_STALL";
	public static final int ICACHE_16B_IFDATA_STALL = 1073741887;
	public static final String ICACHE_16B_IFDATA_STALL_name = "ICACHE_16B:IFDATA_STALL";
	public static final int IDQ_UOPS_NOT_DELIVERED_CYCLES_0_UOPS_DELIV_CORE = 1073741870;
	public static final String IDQ_UOPS_NOT_DELIVERED_CYCLES_0_UOPS_DELIV_CORE_name = "IDQ_UOPS_NOT_DELIVERED:CYCLES_0_UOPS_DELIV_CORE";
	public static final int IDQ_UOPS_NOT_DELIVERED_CYCLES_LE_1_UOPS_DELIV_CORE = 1073741888;
	public static final String IDQ_UOPS_NOT_DELIVERED_CYCLES_LE_1_UOPS_DELIV_CORE_name = "IDQ_UOPS_NOT_DELIVERED:CYCLES_LE_1_UOPS_DELIV_CORE";
	public static final int UOPS_EXECUTED_CORE_CYCLES_GE_1 = 1073741891;
	public static final String UOPS_EXECUTED_CORE_CYCLES_GE_1_name = "UOPS_EXECUTED:CORE_CYCLES_GE_1";
	public static final int UOPS_EXECUTED_CORE_CYCLES_GE_4 = 1073741893;
	public static final String UOPS_EXECUTED_CORE_CYCLES_GE_4_name = "UOPS_EXECUTED:CORE_CYCLES_GE_4";
	public static final int PAPI_L1_DCM = -2147483648;
	public static final String PAPI_L1_DCM_name = "PAPI_L1_DCM";
	public static final int PAPI_L2_DCM = -2147483646;
	public static final String PAPI_L2_DCM_name = "PAPI_L2_DCM";
    public static final int PAPI_L1_TCM = -2147483642;
    public static final String PAPI_L1_TCM_name = "PAPI_L1_TCM";
    public static final int PAPI_L2_TCM = -2147483641;
    public static final String PAPI_L2_TCM_name = "PAPI_L2_TCM";
	public static final int PAPI_L3_TCM = -2147483640;
	public static final String PAPI_L3_TCM_name = "PAPI_L3_TCM";
	public static final int PAPI_L1_ICM = -2147483647;
	public static final String PAPI_L1_ICM_name = "PAPI_L1_ICM";
	public static final int PAPI_L2_ICM = -2147483645;
	public static final String PAPI_L2_ICM_name = "PAPI_L2_ICM";
	public static final int PAPI_TLB_IM = -2147483627;
	public static final String PAPI_TLB_IM_name = "PAPI_TLB_IM";
	public static final int PAPI_TLB_DM = -2147483628;
	public static final String PAPI_TLB_DM_name = "PAPI_TLB_DM";

    public static final int PAPI_BR_INS = -2147483593;
    public static final String PAPI_BR_INS_name = "PAPI_BR_INS";
	public static final int PAPI_BR_MSP = -2147483602;
	public static final String PAPI_BR_MSP_name = "PAPI_BR_MSP";
	public static final int PAPI_BR_TKN = -2147483604;
	public static final String PAPI_BR_TKN_name = "PAPI_BR_TKN";
	public static final int PAPI_BR_NTK = -2147483603;
	public static final String PAPI_BR_NTK_name = "PAPI_BR_NTK";

	public static final int PAPI_MEM_WCY = -2147483612;
	public static final String PAPI_MEM_WCY_name = "PAPI_MEM_WCY";
	public static final int PAPI_STL_ICY = -2147483611;
	public static final String PAPI_STL_ICY_name = "PAPI_STL_ICY";
	public static final int PAPI_FUL_ICY = -2147483610;
	public static final String PAPI_FUL_ICY_name = "PAPI_FUL_ICY";
	public static final int PAPI_STL_CCY = -2147483609;
	public static final String PAPI_STL_CCY_name = "PAPI_STL_CCY";
	public static final int PAPI_FUL_CCY = -2147483608;
	public static final String PAPI_FUL_CCY_name = "PAPI_FUL_CCY";

    public static final int PAPI_TOT_INS = -2147483598;
    public static final String PAPI_TOT_INS_name = "PAPI_TOT_INS";
	public static final int PAPI_TOT_CYC = -2147483589;
	public static final String PAPI_TOT_CYC_name = "PAPI_TOT_CYC";

	public static final int CYCLE_ACTIVITY_STALLS_TOTAL = 1073741894;
	public static final String CYCLE_ACTIVITY_STALLS_TOTAL_name = "CYCLE_ACTIVITY:STALLS_TOTAL";

	public static final int perf_PERF_COUNT_HW_CACHE_NODE_READ = 1073741895;
	public static final String perf_PERF_COUNT_HW_CACHE_NODE_READ_name = "perf::PERF_COUNT_HW_CACHE_NODE:READ";

}
