package uk.ac.imperial.lsds.saber.cql.operators.udfs;

// import java.util.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.WindowBatch;
import uk.ac.imperial.lsds.saber.tasks.IWindowAPI;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.buffers.UnboundedQueryBufferFactory;

import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.StopTuple;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.AvgSpeed;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record.Accident;


public class LinearRoadBenchmarkOp implements IOperatorCode {


	private ITupleSchema outputSchema;

    private ConcurrentHashMap<Integer, Accident> accidents;
    private ConcurrentHashMap<Integer, AvgSpeed> avgSpeed;
    private ConcurrentHashMap<Integer, StopTuple> stopMap;

    public LinearRoadBenchmarkOp (
        ConcurrentHashMap<Integer, Accident> acc,
        ConcurrentHashMap<Integer, AvgSpeed> avgSpeed,
        ConcurrentHashMap<Integer, StopTuple> stopMap
        ) {

        this.accidents = acc;
        this.avgSpeed = avgSpeed;
        this.stopMap = stopMap;
    }

    @Override
	public void processData(WindowBatch batch, IWindowAPI api) {

        // select timestamp, XWay, Seg, AVG(speed) as avgSpeed
        //   from SegSpeedStr [ range 300 ]
        // group by XWay, Seg
        // having avgSpeed < 40.0

        IQueryBuffer inputBuffer = batch.getBuffer();
        IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();

		ITupleSchema schema = batch.getSchema();
        int tupleSize = schema.getTupleSize();

        int segMin = Integer.MAX_VALUE;
        int segMax = Integer.MIN_VALUE;
        int xwayMin = Integer.MAX_VALUE;
        int xwayMax = Integer.MIN_VALUE;

        int timeOfLastToll = -1;
        // System.out.println("batch.startPointer: " + batch.getBufferStartPointer() + ", batch.endPointer: " + batch.getBufferEndPointer() + ", numberOfTuples: " + (batch.getBufferEndPointer() - batch.getBufferStartPointer())/tupleSize);

        for (int pointer = batch.getBufferStartPointer(); pointer < batch.getBufferEndPointer(); pointer += tupleSize) {
            // parser tuples
            final long timestamp = inputBuffer.getLong(pointer);
            final int m_iType = inputBuffer.getInt(pointer+8);
            final int m_iTime = inputBuffer.getInt(pointer+12);
            final int m_iVid  = inputBuffer.getInt(pointer+16);
            final int m_iSpeed = inputBuffer.getInt(pointer+20);
            final int m_iXway = inputBuffer.getInt(pointer+24);
            final int m_iLane = inputBuffer.getInt(pointer+28);
            final int m_iDir = inputBuffer.getInt(pointer+32);
            final int m_iSeg = inputBuffer.getInt(pointer+36);
            final int m_iPos = inputBuffer.getInt(pointer+40);
            final int m_iQid = inputBuffer.getInt(pointer+44);
            final int m_iSinit = inputBuffer.getInt(pointer+48);
            final int m_iSend = inputBuffer.getInt(pointer+52);
            final int m_iDow = inputBuffer.getInt(pointer+56);
            final int m_iTod = inputBuffer.getInt(pointer+60);
            final int m_iDay = inputBuffer.getInt(pointer+64);

            boolean possibleAccident = false;
            segMin = Math.min(segMin, m_iSeg);
            segMax = Math.max(segMax, m_iSeg);

            xwayMin = Math.min(xwayMin, m_iXway);
            xwayMax = Math.max(xwayMax, m_iXway);

            if (m_iSpeed == 0) {
                if (stopMap.containsKey(m_iVid)) {
                    StopTuple curr = stopMap.get(m_iVid);
                    if (curr.pos == m_iPos) {
                        curr.count ++;
                        if (curr.count == 4) {
                            possibleAccident = true;
                        }
                    }
                } else {
                    stopMap.put(m_iVid, new StopTuple(m_iPos, 1));
                }
            } else {
                stopMap.put(m_iVid, new StopTuple(m_iPos, 1));
            }

            if (possibleAccident) {
                // signal accident
                int k = Integer.hashCode(m_iXway) * 31 + m_iPos;
                this.accidents.compute(k, new BiFunction<Integer, Accident, Accident>() {
                        @Override
                        public Accident apply(Integer xway, Accident accident) {
                            if (accident == null) {
                                return new Accident(m_iVid, -1, m_iTime);
                            } else {
                                if (accident.vid2 == -1) {
                                    accident.vid2 = m_iVid;
                                } else if (accident.vid1 == -1) {
                                    accident.vid1 = m_iVid;
                                }
                            }
                            return accident;
                        }
                    });
            }

            if (m_iSpeed > 0) {
                int k = Integer.hashCode(m_iXway) * 31 + m_iPos;
                if (this.accidents.containsKey(k)) {
                    Accident a = this.accidents.get(k);
                    if (a.vid1 == m_iVid) {
                        a.vid1 = -1;
                    } else if (a.vid2 == m_iVid) {
                        a.vid2 = -1;
                    }
                }
            }

            // update avg speed
            int k = Integer.hashCode(m_iXway) * 31 + m_iSeg;
            this.avgSpeed.computeIfPresent(k, new BiFunction<Integer, AvgSpeed, AvgSpeed>() {
                    @Override
                    public AvgSpeed apply(Integer integer, AvgSpeed avgSpeed) {
                        avgSpeed.count ++;
                        avgSpeed.speed += m_iSpeed;
                        return avgSpeed;
                    }
                });
            avgSpeed.putIfAbsent(k, new AvgSpeed(m_iSpeed, 1));

            if (m_iTime % 300 == 0 && m_iTime > 0 && timeOfLastToll != m_iTime) {

                for (int seg = segMin; seg < segMax; seg++) {

                    int ks = Integer.hashCode(m_iXway) * 31 + seg;

                    if (avgSpeed.containsKey(ks)) {
                        AvgSpeed avg = avgSpeed.get(ks);
                        double averageSpeed = 0;
                        if (avg.count > 0) {
                            averageSpeed = avg.speed / avg.count;
                        }
                        if (averageSpeed > 40) {
                            double tollAmount = 0;
                            if (accidents.containsKey(ks)) {
                                tollAmount = (2 * avg.count) ^ 2;
                            }
                            // System.out.println("toll is " + tollAmount + " for seg "+ seg + " and xway " + m_iXway);
                            // System.out.println("average speed is " + averageSpeed + " for seg "+ seg + " and xway " + m_iXway);
                        }
                    }
                }
                timeOfLastToll = m_iTime;
            }
        }

        // System.out.println("StopMap.size: " + this.stopMap.size());
        // System.out.println("Accidents.size: " + this.accidents.size());
        // System.out.println("AvgSpeed.size: " + this.avgSpeed.size());
        // // for (Integer key : stopMap.keySet()) {
        // //     StopTuple s = stopMap.get(key);
        // //     System.out.println("StopMap: " + key + ", stopMap.pos: " + s.pos + ", stopMap.time: " + s.count);
        // // }

        // for (Integer key : accidents.keySet()) {
        //     System.out.println("Acc: " + key + ", " + accidents.get(key).toString());
        // }
        // for (Integer key : avgSpeed.keySet()) {
        //     System.out.println("AvgSpeed: " + key + ", " + avgSpeed.get(key).toString());
        // }


        inputBuffer.release();
        /* Reset position for output buffer */
        outputBuffer.close();
        /* Reuse window batch by setting the new buffer and the new schema for the data in this buffer */
        batch.setBuffer(outputBuffer);
        // batch.setSchema(projectedSchema);
        /* Important to set start and end buffer pointers */
        batch.setBufferPointers(0, outputBuffer.limit());
        api.outputWindowBatchResult(batch);
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

    // @Override
    // public ITupleSchema getOutputSchema() {
    //     return outputSchema;
    // }

}
