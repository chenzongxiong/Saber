package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public abstract class InputStream implements LinearRoadBenchmarkQuery {

	ITupleSchema schema = null;
    ITupleSchema sinkSchema = null;
    ITupleSchema interSchema = null;

	QueryApplication application = null;

	public InputStream () {
	}

	public QueryApplication getApplication () {
		return application;
	}

	public abstract void createApplication (QueryConf queryConf, boolean isExecuted);

	public ITupleSchema getSchema () {
		if (schema == null) {
            createSchema ();
		}
		return schema;
	}
    public ITupleSchema getSinkSchema() {
        if (sinkSchema == null)
            createSinkSchema();
        return sinkSchema;
    }
    public ITupleSchema getIntermediateSchema() {
        if (interSchema == null)
            createIntermediateSchema();
        return interSchema;
    }
	// create schema for SABER
	public void createSchema () {
        final int COLUMNS = 16;
        final int contentSize = 8 + 4 * (COLUMNS - 1);

		int [] offsets = new int [COLUMNS]; // one slot for timestamp

        // m_iType, m_iTime, m_iVid, m_iSpeed, m_iXway, m_iLane
        // m_iDir, m_iSeg, m_iPos, m_iQid, m_iSinit, m_iSend
        // m_iDow, m_iTod, m_iDay
        offsets[0] = 0;         // timestamp long
        for (int i = 1; i < COLUMNS; i ++) {
            offsets[i] =  8 + 4*(i-1);
        }
		schema = new TupleSchema (offsets, contentSize);

		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
        schema.setAttributeType (0, PrimitiveType.LONG);
        for (int i = 1; i < COLUMNS; i ++) {
            schema.setAttributeType (i, PrimitiveType.INT);
        }

		schema.setAttributeName (0, "timestamp"); // timestamp
		schema.setAttributeName (1, "m_iType");
		schema.setAttributeName (2, "m_iTime");
		schema.setAttributeName (3, "m_iVid");
		schema.setAttributeName (4, "m_iSpeed");
		schema.setAttributeName (5, "m_iXway");
		schema.setAttributeName (6, "m_iLane");
		schema.setAttributeName (7, "m_iDir");
		schema.setAttributeName (8, "m_iSeg");
		schema.setAttributeName (9, "m_iPos");
		schema.setAttributeName (10, "m_iQid");
		schema.setAttributeName (11, "m_iSinit");
		schema.setAttributeName (12, "m_iSend");
		schema.setAttributeName (13, "m_iDow");
		schema.setAttributeName (14, "m_iTod");
		schema.setAttributeName (15, "m_iDay");
	}

    // create schema for sink
    public void createSinkSchema() {
        final int COLUMNS = 6;
        final int contentSize = 8 + 4 * (COLUMNS - 1);
		int [] offsets = new int [COLUMNS]; // one slot for timestamp
        offsets[0] = 0;         // timestamp long
        for (int i = 1; i < COLUMNS; i ++) {
            offsets[i] =  8 + 4*(i-1);
        }
		sinkSchema = new TupleSchema (offsets, contentSize);

		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
        sinkSchema.setAttributeType (0, PrimitiveType.LONG);
        for (int i = 1; i < COLUMNS; i ++) {
            sinkSchema.setAttributeType (i, PrimitiveType.INT);
        }

		sinkSchema.setAttributeName (0, "timestamp"); // timestamp
		sinkSchema.setAttributeName (1, "m_iTime");
		sinkSchema.setAttributeName (2, "m_iXway");
		sinkSchema.setAttributeName (3, "m_iSeg");
		sinkSchema.setAttributeName (4, "avg_speed");
        sinkSchema.setAttributeName (5, "toll_amount");
    }

    // create schema for intermediate tuples
    public void createIntermediateSchema() {
        final int COLUMNS = 5;
        final int contentSize = 8 + 4 * (COLUMNS - 1);
		int [] offsets = new int [COLUMNS]; // one slot for timestamp
        offsets[0] = 0;         // timestamp long
        for (int i = 1; i < COLUMNS; i ++) {
            offsets[i] =  8 + 4*(i-1);
        }
		interSchema = new TupleSchema (offsets, contentSize);

		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
        interSchema.setAttributeType (0, PrimitiveType.LONG);
        for (int i = 1; i < COLUMNS; i ++) {
            interSchema.setAttributeType (i, PrimitiveType.INT);
        }

		interSchema.setAttributeName (0, "timestamp"); // timestamp
		interSchema.setAttributeName (1, "m_iTime");
		interSchema.setAttributeName (2, "m_iSpeed");
		interSchema.setAttributeName (3, "m_iXway");
		interSchema.setAttributeName (4, "m_iSeg");
    }
}
