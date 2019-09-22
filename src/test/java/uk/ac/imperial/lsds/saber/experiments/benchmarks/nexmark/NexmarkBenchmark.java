package uk.ac.imperial.lsds.saber.experiments.benchmarks.nexmark;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
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
import uk.ac.imperial.lsds.saber.cql.operators.udfs.NexmarkOp;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.CampaignGenerator;
import uk.ac.imperial.lsds.saber.processors.HashMap;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;
import uk.ac.imperial.lsds.saber.LatencyMonitor;

import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;


public class NexmarkBenchmark {

	ITupleSchema schema = null;
	QueryApplication application = null;
    private PAPIHardwareSampler [] taskWorkerPapiSamplers;
    private PAPIHardwareSampler [] circularWorkerPapiSamplers;

    private LatencyMonitor latencyMonitor1;
    private LatencyMonitor latencyMonitor2;

	public NexmarkBenchmark (QueryConf queryConf, PAPIHardwareSampler[] papiSamplers) {
        if (papiSamplers != null) {
            this.taskWorkerPapiSamplers = new PAPIHardwareSampler[SystemConf.THREADS];
            this.circularWorkerPapiSamplers = new PAPIHardwareSampler[SystemConf.NUMBER_OF_CICULAR_WORKERS];

            this.circularWorkerPapiSamplers[0] = papiSamplers[0];
            this.circularWorkerPapiSamplers[1] = papiSamplers[1];
            for (int i = 0; i < this.taskWorkerPapiSamplers.length; i ++) {
                this.taskWorkerPapiSamplers[i] = papiSamplers[i+2];
            }
        }

        createSchema();
		createApplication(queryConf);
	}

    public void createApplication(QueryConf queryConf) {
        System.out.println("[DBG] Create Application");
        long timestampReference = System.nanoTime();
        int windowSize = 2000;
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

        ITupleSchema inputSchema = schema;
		Set<Query> queries = new HashSet<Query>();

		IOperatorCode cpuCode = new NexmarkOp(inputSchema, windowDefinition);
		QueryOperator operator;
		operator = new QueryOperator(cpuCode, null);
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
        Query query1 = new Query(0,
                                 operators,
                                 inputSchema,
                                 windowDefinition,
                                 null,
                                 null,
                                 queryConf,
                                 timestampReference,
                                 this.circularWorkerPapiSamplers);
        queries.add(query1);


		AggregationType [] aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = AggregationType.CNT;

		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = new FloatColumnReference(1); // COUNT(Auction)

        Expression [] groupByAttributes = null;
        groupByAttributes = new Expression [] { new LongColumnReference(1) };
		cpuCode = new Aggregation(windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);
		operators = new HashSet<QueryOperator>();
		operators.add(operator);
        Query query2 = new Query (1, operators, inputSchema, windowDefinition, null, null, queryConf, timestampReference);
        queries.add(query2);
        query1.connectTo(query2);


        application = new QueryApplication(queries);
        application.setup();

        // if (SystemConf.CPU)
        //     query2.setAggregateOperator((IAggregateOperator) cpuCode);

        // this.latencyMonitor1 = query1.getLatencyMonitor();
        // this.latencyMonitor2 = query2.getLatencyMonitor();
    }

    public void stopLatencyMonitor() {
        // this.latencyMonitor1.stop();
        // this.latencyMonitor2.stop();
    }

    // create schema for Nexmark
	public void createSchema () {
        System.out.println("[DBG] Create Schema");
		int [] offsets = new int [5];
		offsets[0] =  0;
		offsets[1] =  8;
		offsets[2] = 16;
		offsets[3] = 24;
		offsets[4] = 32;
		schema = new TupleSchema (offsets, 40);

		schema.setAttributeType(0, PrimitiveType.LONG);
		schema.setAttributeType(1, PrimitiveType.LONG);
		schema.setAttributeType(2, PrimitiveType.LONG);
		schema.setAttributeType(3, PrimitiveType.LONG);
		schema.setAttributeType(4, PrimitiveType.LONG);

		schema.setAttributeName(0, "timestamp"); // timestamp
		schema.setAttributeName(1, "auction");
		schema.setAttributeName(2, "bidder");
		schema.setAttributeName(3, "price");
		schema.setAttributeName(4, "dateTime");
	}
    public QueryApplication getApplication() {
        // System.out.println("[DBG] Get Application: " + application);
        return application;
    }
}
