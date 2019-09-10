package uk.ac.imperial.lsds.saber.cql.operators.udfs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.processors.ThreadMap;

import uk.ac.imperial.lsds.saber.buffers.PartialWindowResults;
import uk.ac.imperial.lsds.saber.buffers.PartialWindowResultsFactory;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTable;
import uk.ac.imperial.lsds.saber.buffers.WindowHashTableFactory;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.StopTuple;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.AvgSpeed;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.Accident;


public class LinearRoadBenchmarkOp implements IOperatorCode {

	private static final boolean DEBUG = false;

	private ITupleSchema outputSchema;
    private ITupleSchema projectedSchema;

    private ConcurrentHashMap<Integer, Accident> accidents;
    private ConcurrentHashMap<Integer, AvgSpeed> avgSpeed;
    private ConcurrentHashMap<Integer, StopTuple> stopMap;

    private Expression[] expressions = null;
	private IPredicate selectPredicate = null;
	private Expression [] groupByAttributes;
	private boolean groupBy = false;

    private AggregationType [] aggregationTypes;
	private FloatColumnReference [] aggregationAttributes;

	private ThreadLocal<float   []> tl_values;
	private ThreadLocal<int     []> tl_counts;
	private ThreadLocal<byte    []> tl_tuplekey;
	private ThreadLocal<boolean []> tl_found;

    private int keyLength, valueLength;

    public LinearRoadBenchmarkOp (
        ConcurrentHashMap<Integer, Accident> acc,
        ConcurrentHashMap<Integer, AvgSpeed> avgSpeed,
        ConcurrentHashMap<Integer, StopTuple> stopMap,
        IPredicate selectPredicate,
        Expression[] expressions,
        ITupleSchema projectedSchema,
        Expression[] groupByAttributes,
        AggregationType [] aggregationTypes,
        FloatColumnReference [] aggregationAttributes
        ) {
        this.accidents = acc;
        this.avgSpeed = avgSpeed;
        this.stopMap = stopMap;
        this.selectPredicate = selectPredicate;
        this.expressions = expressions;
        this.projectedSchema = projectedSchema;
        this.groupByAttributes = groupByAttributes;

        this.keyLength = 0;

        if (this.groupByAttributes != null) {
            groupBy = true;
            for (int i = 0; i < this.groupByAttributes.length; i ++) {
                Expression expr = groupByAttributes[i];
                if (expr instanceof IntExpression) {
                    this.keyLength += 4;
                } else {
                    //
                    System.out.println("only need IntExpression");
                }
            }

            tl_tuplekey = new ThreadLocal <byte []> () {
                    @Override protected byte [] initialValue () {
                        return new byte [keyLength];
                    }
                };

            tl_found = new ThreadLocal<boolean []> () {
                    @Override protected boolean [] initialValue () {
                        return new boolean [1];
                    }
                };

        } else {
            groupBy = false;
        }

        this.aggregationAttributes = aggregationAttributes;
        this.aggregationTypes = aggregationTypes;
    }

    @Override
	public void processData(WindowBatch batch, IWindowAPI api) {
        // SELECT timestamp, m_iTime, AVG(m_iSpeed), m_iXway, m_iSeg
        // FROM TBL
        // GROUP BY m_iXway, m_iSeg

        if (this.selectPredicate != null) {
            this.select(batch, api);
        }

        // if (this.expressions != null) {
        //     this.project(batch, api);
        // }
        // aggregate(batch, api);

        // IQueryBuffer inputBuffer = batch.getBuffer();
        // IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		// ITupleSchema schema = batch.getSchema();
        // int tupleSize = schema.getTupleSize();

        // System.out.println("batch.startPointer: " + batch.getBufferStartPointer() + ", batch.endPointer: " + batch.getBufferEndPointer() + ", numberOfTuples: " + (batch.getBufferEndPointer() - batch.getBufferStartPointer())/tupleSize);

        // for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
        //     // parser tuples
        //     final long timestamp = inputBuffer.getLong(pointer);
        //     // final int m_iType = inputBuffer.getInt(pointer+8);
        //     final int m_iTime = inputBuffer.getInt(pointer+8);
        //     // final int m_iVid  = inputBuffer.getInt(pointer+16);
        //     final int m_iSpeed = inputBuffer.getInt(pointer+12);
        //     final int m_iXway = inputBuffer.getInt(pointer+16);
        //     // final int m_iLane = inputBuffer.getInt(pointer+28);
        //     // final int m_iDir = inputBuffer.getInt(pointer+32);
        //     final int m_iSeg = inputBuffer.getInt(pointer+20);
        //     // final int m_iPos = inputBuffer.getInt(pointer+40);
        //     // final int m_iQid = inputBuffer.getInt(pointer+44);
        //     // final int m_iSinit = inputBuffer.getInt(pointer+48);
        //     // final int m_iSend = inputBuffer.getInt(pointer+52);
        //     // final int m_iDow = inputBuffer.getInt(pointer+56);
        //     // final int m_iTod = inputBuffer.getInt(pointer+60);
        //     // final int m_iDay = inputBuffer.getInt(pointer+64);

        //     // boolean possibleAccident = false;
        //     // segMin = Math.min(segMin, m_iSeg);
        //     // segMax = Math.max(segMax, m_iSeg);

        //     // xwayMin = Math.min(xwayMin, m_iXway);
        //     // xwayMax = Math.max(xwayMax, m_iXway);
        //     System.out.println("m_iTime: " + m_iTime+ ", m_iSpeed: " + m_iSpeed + ", m_iXway: " + m_iXway + ", m_iSeg: " + m_iSeg);

        //     // if (m_iType != 0) {
        //     //     throw new IllegalStateException("m_iType must be 0");
        //     // } else {
        //     //     // System.out.println("m_iType: " + m_iType);
        //     // }
        // }
		// byte [] tupleKey = (byte []) tl_tuplekey.get(); // new byte [keyLength];
        // int offset = 0;
        // System.out.println("tupleKey.length: " + tupleKey.length);

        // setGroupByKey(inputBuffer, schema, offset, tupleKey);

        // POST-setup
        // inputBuffer.release();
        // outputBuffer.close();
        // batch.setBuffer(outputBuffer);
        // batch.setBufferPointers(0, outputBuffer.limit());
        // api.outputWindowBatchResult(batch);
    }

    private void select(WindowBatch batch, IWindowAPI api) {
        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();
        int processCnt = 0;

        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
            // if (true) {
            if (this.selectPredicate.satisfied(inputBuffer, schema, pointer)) {
                inputBuffer.appendBytesTo(pointer, tupleSize, outputBuffer);
                processCnt ++;
            }
        }

        inputBuffer.release();
        batch.setBuffer(outputBuffer);
        batch.setSchema(schema);

        // if (DEBUG) {
		// 	System.out.println(
		// 		String.format("[DBG] select task %6d: batch starts at %10d (%10d) ends at %10d (%10d)", batch.getTaskId(),
		// 		batch.getBufferStartPointer(),
		// 		batch.getStreamStartPointer(),
		// 		batch.getBufferEndPointer(),
		// 		batch.getStreamEndPointer()
		// 		)
		// 	);
        // }

        batch.setBufferPointers(0, processCnt * tupleSize);
        // batch.setBufferPointers(0, outputBuffer.limit());
        // outputBuffer.close();

        // if (DEBUG) {
		// 	System.out.println(
		// 		String.format("[DBG] select task update %6d: batch starts at %10d (%10d) ends at %10d (%10d)", batch.getTaskId(),
		// 		batch.getBufferStartPointer(),
		// 		batch.getStreamStartPointer(),
		// 		batch.getBufferEndPointer(),
		// 		batch.getStreamEndPointer()
		// 		)
		// 	);
        //     System.out.println("[DBG] selection perform counts: " + processCnt);
        // }

        api.outputWindowBatchResult(batch);
    }

    private void project(WindowBatch batch, IWindowAPI api) {
        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

        ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();
        int processCnt = 0;

        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
			for (int i = 0; i < this.expressions.length; ++i) {
				this.expressions[i].appendByteResult(inputBuffer, schema, pointer, outputBuffer);
			}
            outputBuffer.put(projectedSchema.getPad());
            processCnt ++;
        }

        inputBuffer.release();
        batch.setBuffer(outputBuffer);
		batch.setSchema(projectedSchema);

        batch.setBufferPointers(0, processCnt * projectedSchema.getTupleSize());
        api.outputWindowBatchResult(batch);
    }

    private void aggregate(WindowBatch batch, IWindowAPI api) {
        if (DEBUG) {
            System.out.println("[DBG] Start to aggregation");
        }

        batch.initPartialWindowPointers();

        if (DEBUG) {
			System.out.println(
				String.format("[DBG] aggregation task %6d: batch starts at %10d (%10d) ends at %10d (%10d)", batch.getTaskId(),
				batch.getBufferStartPointer(),
				batch.getStreamStartPointer(),
				batch.getBufferEndPointer(),
				batch.getStreamEndPointer()
				)
			);
        }
        // if (this.groupByAttributes != null) {
        //     this.processDataPerWindowWithGroupBy (batch, api);
        // }
        if (this.groupBy) {
            System.out.println("GroupBy");
        } else {
            System.out.println("No GroupBy");
        }
        // if (DEBUG) {
		// 	System.out.println(
		// 		String.format("[DBG] aggregation task %6d: %4d closing %4d pending %4d complete and %4d opening windows]", batch.getTaskId(),
		// 			batch.getClosingWindows ().numberOfWindows(),
		// 			batch.getPendingWindows ().numberOfWindows(),
		// 			batch.getCompleteWindows().numberOfWindows(),
		// 			batch.getOpeningWindows ().numberOfWindows()
		// 		)
		// 	);
        // }
        // batch.getBuffer().release();
        // api.outputWindowBatchResult(batch);
    }

    public boolean hasGroupBy() {
        return groupBy;
    }
    @Override
	public void processData(WindowBatch first, WindowBatch second, IWindowAPI api) {
        throw new UnsupportedOperationException("error: operator does not operate on two streams");
    }

    @Override
	public void configureOutput(int queryId) {
        throw new UnsupportedOperationException("error: `configureOutput` method is applicable only to GPU operators");
    }

    @Override
	public void processOutput(int queryId, WindowBatch batch) {
        throw new UnsupportedOperationException("error: `processOutput` method is applicable only to GPU operators");
    }

    @Override
	public void setup() {
		throw new UnsupportedOperationException("error: `setup` method is applicable only to GPU operators");
    }

    private void setGroupByKey (IQueryBuffer buffer, ITupleSchema schema, int offset, byte [] bytes) {
        int pivot = 0;
        for (int i = 0; i < this.groupByAttributes.length; i ++) {
            pivot = this.groupByAttributes[i].evalAsByteArray(buffer, schema, offset, bytes, pivot);
        }

    }

	private void processDataPerWindowWithGroupBy (WindowBatch batch, IWindowAPI api) {

		int workerId = ThreadMap.getInstance().get(Thread.currentThread().getId());

		int [] startP = batch.getWindowStartPointers();
		int []   endP = batch.getWindowEndPointers();

		ITupleSchema inputSchema = batch.getSchema();
		int inputTupleSize = inputSchema.getTupleSize();

		PartialWindowResults  closingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  pendingWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults completeWindows = PartialWindowResultsFactory.newInstance (workerId);
		PartialWindowResults  openingWindows = PartialWindowResultsFactory.newInstance (workerId);

		IQueryBuffer inputBuffer = batch.getBuffer();
		IQueryBuffer outputBuffer = null;

		/* Current window start and end pointers */
		int start, end;

		WindowHashTable windowHashTable = null;
		byte [] tupleKey = (byte []) tl_tuplekey.get(); // new byte [keyLength];
		boolean [] found = (boolean []) tl_found.get(); // new boolean[1];
		boolean pack = false;
        System.out.println("startP.length: " + startP.length + ", endP.length: " + endP.length);
        // for (int i = 0; i < startP.length; i ++) {
        //     System.out.println("startP[" + i + "]: " + startP[i] + ", endP[" + i + "]:" + endP[i]);
        // }

		// float [] values = tl_values.get();

		for (int currentWindow = 0; currentWindow < startP.length; ++currentWindow) {
			if (currentWindow > batch.getLastWindowIndex())
				break;

			pack = false;

			start = startP [currentWindow];
			end   = endP   [currentWindow];
            System.out.println("start: " + start + ", end: " + end);
			/* Check start and end pointers */
			if (start < 0 && end < 0) {
				start = batch.getBufferStartPointer();
				end = batch.getBufferEndPointer();
				if (batch.getStreamStartPointer() == 0) {
					/* Treat this window as opening; there is no previous batch to open it */
					outputBuffer = openingWindows.getBuffer();
					openingWindows.increment();
				} else {
					/* This is a pending window; compute a pending window once */
					if (pendingWindows.numberOfWindows() > 0)
						continue;
					outputBuffer = pendingWindows.getBuffer();
					pendingWindows.increment();
				}
			} else if (start < 0) {
				outputBuffer = closingWindows.getBuffer();
				closingWindows.increment();
				start = batch.getBufferStartPointer();
			} else if (end < 0) {
				outputBuffer = openingWindows.getBuffer();
				openingWindows.increment();
				end = batch.getBufferEndPointer();
			} else {
				if (start == end) /* Empty window */
					continue;

				outputBuffer = completeWindows.getBuffer();
				completeWindows.increment();
				pack = true;
			}
			/* If the window is empty, skip it */
			if (start == -1)
				continue;

			windowHashTable = WindowHashTableFactory.newInstance(workerId);
			windowHashTable.setTupleLength(keyLength, valueLength);

			// while (start < end) {
			// 	/* Get the group-by key */
			// 	setGroupByKey (inputBuffer, inputSchema, start, tupleKey);
			// 	/* Get values */
			// 	for (int i = 0; i < numberOfValues(); ++i) {
			// 		if (aggregationTypes[i] == AggregationType.CNT)
			// 			values[i] = 1;
			// 		else {
			// 			if(inputBuffer == null)
			// 				System.err.println("Input is iull?");
			// 			if(inputSchema == null)
			// 				System.err.println("Schema is null?");
			// 			if(values == null)
			// 				System.err.println("Values is null?");
			// 			if(aggregationAttributes[i] == null)
			// 				System.err.println("Attributes is null?");
			// 			values[i] = aggregationAttributes[i].eval (inputBuffer, inputSchema, start);

			// 		}
			// 	}

			// 	/* Check whether there is already an entry in the hash table for this key.
			// 	 * If not, create a new entry */
			// 	found[0] = false;
			// 	int idx = windowHashTable.getIndex (tupleKey, found);
			// 	if (idx < 0) {
			// 		System.out.println("error: open-adress hash table is full");
			// 		System.exit(1);
			// 	}

			// 	ByteBuffer theTable = windowHashTable.getBuffer();
			// 	if (! found[0]) {
			// 		theTable.put (idx, (byte) 1);
			// 		int timestampOffset = windowHashTable.getTimestampOffset (idx);
			// 		theTable.position (timestampOffset);
			// 		/* Store timestamp */
			// 		theTable.putLong (inputBuffer.getLong(start));
			// 		/* Store key and value(s) */
			// 		theTable.put (tupleKey);
			// 		for (int i = 0; i < numberOfValues(); ++i)
			// 			theTable.putFloat(values[i]);
			// 		/* Store count */
			// 		theTable.putInt(1);
			// 	} else {
			// 		/* Update existing entry */
			// 		int valueOffset = windowHashTable.getValueOffset (idx);
			// 		// int countOffset = windowHashTable.getCountOffset (idx);
			// 		/* Store value(s) */
			// 		float v;
			// 		int p;
			// 		for (int i = 0; i < numberOfValues(); ++i) {
			// 			p = valueOffset + i * 4;
			// 			switch (aggregationTypes[i]) {
			// 			// case CNT:
			// 			// 	theTable.putFloat(p, (theTable.getFloat(p) + 1));
			// 			// 	break;
			// 			// case SUM:
			// 			case AVG:
			// 				theTable.putFloat(p, (theTable.getFloat(p) + values[i]));
			// 			// case MIN:
			// 			// 	v = theTable.getFloat(p);
			// 			// 	theTable.putFloat(p, ((v > values[i]) ? values[i] : v));
			// 			// 	break;
			// 			// case MAX:
			// 			// 	v = theTable.getFloat(p);
			// 			// 	theTable.putFloat(p, ((v < values[i]) ? values[i] : v));
			// 			// 	break;
			// 			default:
			// 				throw new IllegalArgumentException ("error: invalid aggregation type");
			// 			}
			// 		}
			// 		/* Increment tuple count */
			// 		theTable.putInt(countOffset, theTable.getInt(countOffset) + 1);
			// 	}
			// 	/* Move to next tuple in window */
			// 	start += inputTupleSize;
			// }
			/* Store window result and move to next window */
			evaluateWindow (windowHashTable, outputBuffer, pack);
			/* Release hash maps */
			// windowHashTable.release();
		}

		/* At the end of processing, set window batch accordingly */
		batch.setClosingWindows  ( closingWindows);
		batch.setPendingWindows  ( pendingWindows);
		batch.setCompleteWindows (completeWindows);
		batch.setOpeningWindows  ( openingWindows);
	}

	private void evaluateWindow (WindowHashTable windowHashTable, IQueryBuffer buffer, boolean pack) {

// 		/* Write current window results to output buffer; copy the entire hash table */
// 		if (! pack) {
// 			buffer.put(windowHashTable.getBuffer().array());
// 			return;
// 		}

// 		/* Set complete windows */
// /*		System.out.println("Complete windows start at " + buffer.position());
// */
// 		ByteBuffer theTable = windowHashTable.getBuffer();
// 		int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();
// 		/* Pack the elements of the table */

// /*		int tupleIndex = 0;
// 		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {
// 			if (theTable.get(idx) == 1) {
// 				int mark = theTable.get(idx);
// 				long timestamp = theTable.getLong(idx + 8);
// 				long fKey = theTable.getLong(idx + 16);
// 				long key = theTable.getLong(idx + 24);
// 				float val1 = theTable.getFloat(idx + 32);
// 				float val2 = theTable.getFloat(idx + 36);
// 				int count = theTable.getInt(idx + 40);
// 				System.out.println(String.format("%5d: %10d, %10d, %10d, %10d, %5.3f, %5.3f, %10d",
// 						tupleIndex,
// 						mark,
// 						timestamp,
// 						fKey,
// 						key,
// 						val1,
// 						val2,
// 						count
// 						));
// 			}
// 			tupleIndex ++;
// 		}*/

// 		//System.exit(1);
// //		int tupleIndex = 0;
// //		for (int idx = offset; idx < (offset + SystemConf.HASH_TABLE_SIZE); idx += 32) {
// //			int mark = buffer.getInt(idx + 0);
// //			if (mark > 0) {
// //				long timestamp = buffer.getLong(idx + 8);
// //				//
// //				// int key_1
// //				// float value1
// //				// float value2
// //				// int count
// //				//
// //				int key = buffer.getInt(idx + 16);
// //				float val1 = buffer.getFloat(idx + 20);
// //				float val2 = buffer.getFloat(idx + 24);
// //				int count = buffer.getInt(idx + 28);
// //				System.out.println(String.format("%5d: %10d, %10d, %10d, %5.3f, %5.3f, %10d",
// //					tupleIndex,
// //					Integer.reverseBytes(mark),
// //					Long.reverseBytes(timestamp),
// //					Integer.reverseBytes(key),
// //					0F,
// //					0F,
// //					Integer.reverseBytes(count)
// //				));
// //			}
// //			tupleIndex ++;
// //		}

// 		// ByteBuffer theTable = windowHashTable.getBuffer();
// 		// int intermediateTupleSize = windowHashTable.getIntermediateTupleSize();

// 		/* Pack the elements of the table */
// 		for (int idx = 0; idx < theTable.capacity(); idx += intermediateTupleSize) {

// 			if (theTable.get(idx) != 1) /* Skip empty slots */
// 				continue;

// 			/* Store timestamp, and key */
// 			int timestampOffset = windowHashTable.getTimestampOffset (idx);
// 			buffer.put(theTable.array(), timestampOffset, (8 + keyLength));

// 			int valueOffset = windowHashTable.getValueOffset (idx);
// 			int countOffset = windowHashTable.getCountOffset (idx);

// 			int count = theTable.getInt(countOffset);
// 			int p;
// 			for (int i = 0; i < numberOfValues(); ++i) {
// 				p = valueOffset + i * 4;
// 				if (aggregationTypes[i] == AggregationType.AVG) {
// 					buffer.putFloat(theTable.getFloat(p) / (float) count);
// 				} else {
// 					buffer.putFloat(theTable.getFloat(p));
// 				}
// 			}
// 			buffer.put(outputSchema.getPad());
// 		}
	}

	public int numberOfValues () {
		// return aggregationAttributes.length;
        return 0;
	}

    @Override
    public String toString () {
        final StringBuilder s = new StringBuilder();
        s.append(selectPredicate.toString());
        return s.toString();
    }
}
