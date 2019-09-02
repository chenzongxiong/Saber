package uk.ac.imperial.lsds.saber.experiments.benchmarks.lrb.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import uk.ac.imperial.lsds.saber.devices.TheCPU;
import uk.ac.imperial.lsds.saber.hardware.papi.PAPIHardwareSampler;
import uk.ac.imperial.lsds.saber.SystemConf;


public class GeneratorWorker implements Runnable {

	Generator generator;
	volatile boolean started = false;

	private int isFirstTime = 2;
	private final int startPos;
	private final int endPos;
	private final int id;

    private String line = null;
    private ArrayList<String> lines;

    // private int index = 0;

    public long consumedTuples = 0;
    private final int tuplesPerBuffer;
    public ByteBuffer tmpBuffer;
    private final static int TUPLE_SIZE = 128;

	private AtomicInteger index;
    // private int prevStartPos = 0;
    // private int prevEndPos = 0;

	public GeneratorWorker (Generator generator, int startPos, int endPos, int id, ArrayList<String> lines) {
		this.generator = generator;
        // this.prevStartPos = startPos;
        // this.prevEndPos = endPos;
		this.startPos = startPos;
		this.endPos = endPos;
		this.id = id;
        this.lines = lines;

        this.tuplesPerBuffer = (endPos - startPos) / TUPLE_SIZE;

        this.tmpBuffer = ByteBuffer.allocate(TUPLE_SIZE*lines.size());
        // System.out.println("[DBG] Allocate size " + this.tmpBuffer.limit() + ", position: " + this.tmpBuffer.position());
        this._fill();
	}

	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {

	}

	@Override
	public void run() {
		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind Worker Generator thread %2d to core %2d", id, id));

		int curr;
		GeneratedBuffer buffer;
		int prev = 0;
		long timestamp;

		// started = true;
        try {
            while (true) {

                while ((curr = generator.next) == prev)
                    ;

                // System.out.println("Filling buffer " + curr);

                buffer = generator.getBuffer (curr);

                /* Fill buffer... */
                timestamp = generator.getTimestamp ();

                generate(buffer, startPos, endPos, timestamp);

                buffer.decrementLatch ();
                prev = curr;
                this.consumedTuples += this.tuplesPerBuffer;
                // if (this.consumedTuples >= 20*1000000) {
                //     System.out.println("Already processed " + this.consumedTuples + " tuples .");
                //     break;
                // }
                // System.out.println("done filling buffer " + curr);
                // break;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}

	private void generate(GeneratedBuffer generatedBuffer, int startPos, int endPos, long timestamp) {

        ByteBuffer buffer = generatedBuffer.getBuffer().duplicate();
		/* Fill the buffer */
        // TODO: seek to probablity position
        ByteBuffer _tmp = this.tmpBuffer.duplicate();

        _tmp.position(startPos);
        _tmp.limit(endPos);
        buffer.put(_tmp);

        buffer.position(startPos);
        buffer.limit(endPos);
        // System.out.println("id: " + this.id + ", buffer.position: " + buffer.position() + ", buffer.limit: " + buffer.limit() + ", startPos: " + startPos + ", endPos: " + endPos);
	}

    private void _fill() {
        for (int i = 0; i < this.lines.size(); i ++) {
            this.tmpBuffer.putLong(0);
            this.line = this.lines.get(i);
            String[] tokens = this.line.split(",");

            for (int j = 0; j < 15; j ++) {
                this.tmpBuffer.putInt(Integer.parseInt(tokens[j]));
            }

            this.tmpBuffer.position(this.tmpBuffer.position() + 60); // padding to 128 bytes
        }

    }
}
