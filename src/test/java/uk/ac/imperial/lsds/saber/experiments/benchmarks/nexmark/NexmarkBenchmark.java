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
import uk.ac.imperial.lsds.saber.cql.operators.udfs.NexmarkOp1;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.NexmarkOp2;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.NexmarkOp3;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.NexmarkOp4;
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

	ITupleSchema inputSchema = null;
    ITupleSchema outputSchema = null;

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
        createOutputSchema();
        createQ1(queryConf);
        // createQ2(queryConf);
        // createQ3(queryConf);
        // createQ4(queryConf);
	}

    public void createApplication(QueryConf queryConf) {
        System.out.println("[DBG] Create Application");
        long timestampReference = System.nanoTime();
        int windowSize = 2000;
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

        // ITupleSchema inputSchema = schema;
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
        operator = new QueryOperator(cpuCode, null);
		operators = new HashSet<QueryOperator>();
		operators.add(operator);
        Query query2 = new Query(0,
                                 operators,
                                 inputSchema,
                                 windowDefinition,
                                 null,
                                 null,
                                 queryConf,
                                 timestampReference);
        queries.add(query2);
        query1.connectTo(query2);

        application = new QueryApplication(queries);
        application.setup();

        if (SystemConf.CPU)
            query2.setAggregateOperator((IAggregateOperator) cpuCode);

        // this.latencyMonitor1 = query1.getLatencyMonitor();
        // this.latencyMonitor2 = query2.getLatencyMonitor();
    }

    public void createQ1(QueryConf queryConf) {
        System.out.println("[DBG] Create Application");
        long timestampReference = System.nanoTime();
        int windowSize = 2000;
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

		Set<Query> queries = new HashSet<Query>();

		IOperatorCode cpuCode = new NexmarkOp1(inputSchema, windowDefinition, outputSchema);
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
        application = new QueryApplication(queries);
        application.setup();
    }

    public void createQ2(QueryConf queryConf) {
        System.out.println("[DBG] Create Application");
        long timestampReference = System.nanoTime();
        int windowSize = 2000;
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

		Set<Query> queries = new HashSet<Query>();

		IOperatorCode cpuCode = new NexmarkOp2(inputSchema, windowDefinition, outputSchema);
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
        application = new QueryApplication(queries);
        application.setup();
    }

    public void createQ3(QueryConf queryConf) {

        System.out.println("[DBG] Create Application");
        long timestampReference = System.nanoTime();
        int windowSize = 2000;
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

		Set<Query> queries = new HashSet<Query>();


		IOperatorCode cpuCode0 = new NexmarkOp3(inputSchema, windowDefinition);
		QueryOperator operator0 = new QueryOperator(cpuCode0, null);
		Set<QueryOperator> operators0 = new HashSet<QueryOperator>();

		operators0.add(operator0);
        Query query0 = new Query(0,
                                 operators0,
                                 inputSchema,
                                 windowDefinition,
                                 null,
                                 null,
                                 queryConf,
                                 timestampReference,
                                 this.circularWorkerPapiSamplers);
        ////////////////////////////////////////////////////////////////////////////////
		AggregationType [] aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = AggregationType.CNT;
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = new FloatColumnReference(1); // COUNT(Auction)
        Expression [] groupByAttributes = null;
        groupByAttributes = new Expression [] { new LongColumnReference(1) }; // Group(Auction)
        IOperatorCode cpuCode = new Aggregation(windowDefinition,
                                                aggregationTypes,
                                                aggregationAttributes,
                                                groupByAttributes);
        QueryOperator operator = new QueryOperator(cpuCode, null);
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
        Query query1 = new Query(0,
                                 operators,
                                 inputSchema,
                                 windowDefinition,
                                 null,
                                 null,
                                 queryConf,
                                 timestampReference);
        ////////////////////////////////////////////////////////////////////////////////
		AggregationType [] aggregationTypes2 = new AggregationType [1];
		aggregationTypes2[0] = AggregationType.MAX;
		FloatColumnReference[] aggregationAttributes2 = new FloatColumnReference [1];
		aggregationAttributes2[0] = new FloatColumnReference(1); // MAX(Auction)
        // Expression [] groupByAttributes2 = new Expression [] { new LongColumnReference(1) }; // Group(Auction)
        IOperatorCode cpuCode2 = new Aggregation(windowDefinition,
                                                 aggregationTypes2,
                                                 aggregationAttributes2,
                                                 null);
        QueryOperator operator2 = new QueryOperator(cpuCode2, null);
		Set<QueryOperator> operators2 = new HashSet<QueryOperator>();
		operators2.add(operator2);
        Query query2 = new Query(1,
                                 operators2,
                                 inputSchema,
                                 windowDefinition,
                                 null,
                                 null,
                                 queryConf,
                                 timestampReference);

        // queries.add(query0);
        queries.add(query1);
        queries.add(query2);
        query1.connectTo(query2);
        // query0.connectTo(query1);

        application = new QueryApplication(queries);
        application.setup();

        if (SystemConf.CPU) {
            query1.setAggregateOperator((IAggregateOperator) cpuCode);
            query2.setAggregateOperator((IAggregateOperator) cpuCode2);
        }
    }

    public void createQ4(QueryConf queryConf) {

        System.out.println("[DBG] Create Application");
        long timestampReference = System.nanoTime();
        int windowSize = 2000;
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);

		Set<Query> queries = new HashSet<Query>();

        ////////////////////////////////////////////////////////////////////////////////
		// IOperatorCode cpuCode0 = new NexmarkOp3(inputSchema, windowDefinition);
		// QueryOperator operator0 = new QueryOperator(cpuCode0, null);
		// Set<QueryOperator> operators0 = new HashSet<QueryOperator>();

		// operators0.add(operator0);
        // Query query0 = new Query(0,
        //                          operators0,
        //                          inputSchema,
        //                          windowDefinition,
        //                          null,
        //                          null,
        //                          queryConf,
        //                          timestampReference,
        //                          this.circularWorkerPapiSamplers);
        ////////////////////////////////////////////////////////////////////////////////
		AggregationType [] aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = AggregationType.CNT;
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = new FloatColumnReference(3); // COUNT(Auction)
        Expression [] groupByAttributes = null;
        groupByAttributes = new Expression [] { new LongColumnReference(1) }; // Group(Auction)
        IOperatorCode cpuCode = new Aggregation(windowDefinition,
                                                aggregationTypes,
                                                aggregationAttributes,
                                                groupByAttributes);
        QueryOperator operator = new QueryOperator(cpuCode, null);
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);
        Query query1 = new Query(0,
                                 operators,
                                 inputSchema,
                                 windowDefinition,
                                 null,
                                 null,
                                 queryConf,
                                 timestampReference);
        ////////////////////////////////////////////////////////////////////////////////
		// AggregationType [] aggregationTypes2 = new AggregationType [1];
		// aggregationTypes2[0] = AggregationType.MAX;
		// FloatColumnReference[] aggregationAttributes2 = new FloatColumnReference [1];
		// aggregationAttributes2[0] = new FloatColumnReference(3); // MAX(price)
        // IOperatorCode cpuCode2 = new Aggregation(windowDefinition,
        //                                          aggregationTypes2,
        //                                          aggregationAttributes2,
        //                                          null);
        // QueryOperator operator2 = new QueryOperator(cpuCode2, null);
		// Set<QueryOperator> operators2 = new HashSet<QueryOperator>();
		// operators2.add(operator2);
        // Query query2 = new Query(0,
        //                          operators2,
        //                          inputSchema,
        //                          windowDefinition,
        //                          null,
        //                          null,
        //                          queryConf,
        //                          timestampReference);

        // queries.add(query0);
        queries.add(query1);
        // queries.add(query2);
        // query1.connectTo(query2);
        // query0.connectTo(query1);

        application = new QueryApplication(queries);
        application.setup();

        if (SystemConf.CPU) {
            query1.setAggregateOperator((IAggregateOperator) cpuCode);
            // query2.setAggregateOperator((IAggregateOperator) cpuCode2);
        }
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
		inputSchema = new TupleSchema (offsets, 40);

		inputSchema.setAttributeType(0, PrimitiveType.LONG);
		inputSchema.setAttributeType(1, PrimitiveType.LONG);
		inputSchema.setAttributeType(2, PrimitiveType.LONG);
		inputSchema.setAttributeType(3, PrimitiveType.LONG);
		inputSchema.setAttributeType(4, PrimitiveType.LONG);

		inputSchema.setAttributeName(0, "timestamp"); // timestamp
		inputSchema.setAttributeName(1, "auction");
		inputSchema.setAttributeName(2, "bidder");
		inputSchema.setAttributeName(3, "price");
		inputSchema.setAttributeName(4, "dateTime");
	}
    public void createOutputSchema() {
        int [] offsets = new int[3];
		offsets[0] =  0;
		offsets[1] =  8;
		offsets[2] = 16;
		outputSchema = new TupleSchema (offsets, 24);

		outputSchema.setAttributeType(0, PrimitiveType.LONG);
		outputSchema.setAttributeType(1, PrimitiveType.LONG);
		outputSchema.setAttributeType(2, PrimitiveType.LONG);

		outputSchema.setAttributeName(0, "timestamp"); // timestamp
		outputSchema.setAttributeName(1, "auction");
		outputSchema.setAttributeName(2, "price");

    }
    public QueryApplication getApplication() {
        // System.out.println("[DBG] Get Application: " + application);
        return application;
    }
}
