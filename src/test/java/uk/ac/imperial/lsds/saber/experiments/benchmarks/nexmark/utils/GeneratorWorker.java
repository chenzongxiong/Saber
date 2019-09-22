package uk.ac.imperial.lsds.saber.experiments.benchmarks.nexmark.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

import uk.ac.imperial.lsds.saber.devices.TheCPU;

// ./nexmark_data_generator 100000000 10000 100000
// number of tuples, number of person, number of auctions, max price == number of person
public class GeneratorWorker implements Runnable {

	Generator generator;
	volatile boolean started = false;

	private int isFirstTime = 2;

	private final int startPos;
	private final int endPos;
	private final int id;

    public int generatedTuples = 0;
    private int tuplesPerBuffer = -1;

	public GeneratorWorker (Generator generator, int startPos, int endPos, int id) {
		this.generator = generator;

		this.startPos = startPos;
		this.endPos = endPos;
		this.id = id;

        this.tuplesPerBuffer = (endPos + 1 - startPos) / 64;
	}

	/*
	 * Pass start/end pointers here...
	 */
	public void configure () {

	}

	@Override
	public void run() {

		TheCPU.getInstance().bind(id);
		System.out.println(String.format("[DBG] bind YSB Worker Generator thread %2d to core %2d", id, id));

		int curr;
		GeneratedBuffer buffer;
		int prev = 0;
		long timestamp;

		started = true;

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
			// System.out.println("done filling buffer " + curr);
			// break;
            this.generatedTuples += this.tuplesPerBuffer;
		}
		// System.out.println("worker exits " );
	}

	private void generate(GeneratedBuffer generatedBuffer, int startPos, int endPos, long timestamp) {

		ByteBuffer buffer = generatedBuffer.getBuffer().duplicate();
		/* Fill the buffer */

		if (isFirstTime!=0 ) {

			buffer.position(startPos);
			while (buffer.position()  < endPos) {
                // long auction = generateAuction(0, 100000);
                // long person = generatePerson(0, 10000);
                // long price = generatePrice(0, 10000);

                long auction = 1;
                long person = 2;
                long price = 3;

			    buffer.putLong (timestamp);
                buffer.putLong(auction);
                buffer.putLong(person);
                buffer.putLong(price);
                buffer.putLong(timestamp);

                buffer.position(buffer.position() + 24);
			}
			isFirstTime --;
		} else {
			buffer.position(startPos);
			while (buffer.position()  < endPos) {

			    buffer.putLong (timestamp);

				// buffer padding
				buffer.position(buffer.position() + 56);
			}
		}
	}

    private long generatePrice(long min, long max) {
        return generateHelper(min, max);
    }
    private long generateAuction(long min, long max) {
        return generateHelper(min, max);
    }
    private long generatePerson(long min, long max) {
        return generateHelper(min, max);
    }
    private long generateHelper(long min, long max) {
        return (long)((Math.random() * ((max - min) + 1)) + min);
    }
}
