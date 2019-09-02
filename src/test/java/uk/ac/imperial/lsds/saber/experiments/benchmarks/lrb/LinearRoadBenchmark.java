package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.YahooBenchmarkOp;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;

import uk.ac.imperial.lsds.saber.cql.operators.udfs.LinearRoadBenchmarkOp;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.StopTuple;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.AvgSpeed;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.Accident;

import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;
import uk.ac.imperial.lsds.saber.LatencyMonitor;


public class LinearRoadBenchmark extends InputStream {
    // SELELCT timestamp, m_iTime, xway, seg, AVG(speed), tollAmount
    // FROM TABLE RANGE IN 300
    // GROUP BY xway, seg

    private PAPIHardwareSampler[] circularWorkerPapiSamplers;
    private PAPIHardwareSampler[] taskWorkerPapiSamplers;

    private LatencyMonitor latencyMonitor1;
    private LatencyMonitor latencyMonitor2;


    public LinearRoadBenchmark (QueryConf queryConf, boolean isExecuted, PAPIHardwareSampler[] papiSamplers) {
        if (papiSamplers != null) {
            this.taskWorkerPapiSamplers = new PAPIHardwareSampler[SystemConf.THREADS];
            this.circularWorkerPapiSamplers = new PAPIHardwareSampler[SystemConf.NUMBER_OF_CICULAR_WORKERS];

            for (int i = 0; i < this.circularWorkerPapiSamplers.length; i ++) {
               this.circularWorkerPapiSamplers[i] = papiSamplers[i];
            }
            for (int i = 0; i < this.taskWorkerPapiSamplers.length; i ++) {
                this.taskWorkerPapiSamplers[i] = papiSamplers[i+SystemConf.NUMBER_OF_CICULAR_WORKERS];
            }
        }
        this.createSchema();
        this.createApplication(queryConf, isExecuted);
    }

	public void createApplication(QueryConf queryConf, boolean isExecuted) {
		/* Set execution parameters */
		long timestampReference = System.nanoTime();
		boolean realtime = true;
		int windowSize = 1000;//realtime? 10000 : 10000000;

		/* Create Input Schema */
		ITupleSchema inputSchema = schema;

        /* FILTER (m_iType == 0) */
        /* 0: position report */
        /* Create the predicates required for the filter operator */
        IPredicate selectPredicate = new IntComparisonPredicate
            (IntComparisonPredicate.EQUAL_OP, new IntColumnReference(1), new IntConstant(0));

        // only perform selection operation
        IOperatorCode selection_code = new Selection((IPredicate) selectPredicate);
        IOperatorCode gpuCode = null;

        QueryOperator operator1;
        operator1 = new QueryOperator (selection_code, null);

        Set<QueryOperator> operators1 = new HashSet<QueryOperator>();
        operators1.add(operator1);
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

        Query query1 = new Query (0, operators1, inputSchema, windowDefinition, null, null, queryConf, timestampReference, this.circularWorkerPapiSamplers);

        ConcurrentHashMap<Integer, Accident> accidents = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, AvgSpeed> avgSpeed = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, StopTuple> stopMap = new ConcurrentHashMap<> ();

        IOperatorCode lrb_code = new LinearRoadBenchmarkOp(
            accidents,
            avgSpeed,
            stopMap
            );

        QueryOperator operator2 = new QueryOperator(lrb_code, null);
        Set<QueryOperator> operators2 = new HashSet<QueryOperator>();
        operators2.add(operator2);
        Query query2 = new Query (1, operators2, inputSchema, windowDefinition, null, null, queryConf, timestampReference);

        Set<Query> queries = new HashSet<Query>();
        queries.add(query1);
        queries.add(query2);
        query1.connectTo(query2);

		if (isExecuted) {
			application = new QueryApplication(queries, this.taskWorkerPapiSamplers);
			application.setup();

            if (SystemConf.LATENCY_ON) {
                this.latencyMonitor1 = query1.getLatencyMonitor();
                this.latencyMonitor2 = query2.getLatencyMonitor();
            }
		}
		return;
	}

    public void stopLatencyMonitor() {
        if (SystemConf.LATENCY_ON) {
            this.latencyMonitor1.stop();
            this.latencyMonitor2.stop();
        }
    }


    Expression [] expressions = null;

    AggregationType [] aggregationTypes = null;
    FloatColumnReference[] aggregationAttributes = null;

    Expression [] groupByAttributes = null;

    private void prepareExpressions () {
        expressions = new Expression[5];
        expressions[0] = new LongColumnReference(0); // timestamp
        expressions[1] = new IntColumnReference(2);  // m_iTime
        expressions[2] = new IntColumnReference(4);  // speed;
        expressions[3] = new IntColumnReference(5);  // xway
        expressions[4] = new IntColumnReference(8);  // seg

        aggregationTypes = new AggregationType [1];
        aggregationTypes[0] = AggregationType.AVG;
        aggregationAttributes = new FloatColumnReference[] { new FloatColumnReference(4) };

        groupByAttributes = new Expression [2];
        groupByAttributes[0] = new IntColumnReference(5);
        groupByAttributes[1] = new IntColumnReference(8);

    }
}
