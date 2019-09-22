package uk.ac.imperial.lsds.saber.experiments.benchmarks.nexmark.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

public class Generator {

	GeneratedBuffer [] buffers;
	volatile int next;

	Executor executor;
	GeneratorWorker [] workers;

	long timestamp = 0;
	long timestampBase = 0;
	long count, limit;

	private final int bufferSize;
	private final int numberOfThreads;

	private List<List<Integer>> positionsList;

	private boolean isV2 = false;
    public int totalGeneratedTuples = 0;
    public Generator(int bufferSize, int numberOfThreads, int coreToBind) {
        this.bufferSize = bufferSize;
        this.numberOfThreads = numberOfThreads;
        buffers = new GeneratedBuffer[2];
        for (int i = 0; i < buffers.length; i ++) {
            buffers[i] = new GeneratedBuffer(bufferSize, false, numberOfThreads);
        }
        next = 0;
        count = 0;
        limit = 1;
        timestampBase = System.currentTimeMillis();
        timestamp = System.currentTimeMillis() - timestampBase;
        int inputTupleSize = 64;
        createPositionsList(inputTupleSize);
        workers = new GeneratorWorker[numberOfThreads];

        for (int i = 0; i < workers.length; i ++) {
            workers[i] = new GeneratorWorker(this, positionsList.get(i).get(0), positionsList.get(i).get(1), i+coreToBind);
            Thread thread = new Thread(workers[i]);
            thread.start();
        }
        fillNext();
    }

	public GeneratedBuffer getBuffer (int id) {
		return buffers[id];
	}

	public int getPointer () {
		return next;
	}

	public long getTimestamp () {
		return timestamp;
	}

	public GeneratedBuffer getNext () throws InterruptedException {
		GeneratedBuffer buffer = buffers[next];
		/* Is buffer `next` generated? */
		while (! buffer.isFilled())
			;//Thread.yield();
		fillNext ();
		/* Lock and return the current buffer */
		return buffer.lock();
	}

	public void fillNext () {
        this.totalGeneratedTuples = 0;
        for (int i = 0; i < this.workers.length; i ++) {
            this.totalGeneratedTuples += this.workers[i].generatedTuples;
        }

		int id;

		/* Set time stamp */
		if (count >= limit) {
			count = 0;
			//System.out.println("Timestamp is " + timestamp);
			timestamp = System.currentTimeMillis() - timestampBase;
			//timestamp ++;
		}
		/* Buffer swap */
		id = (next + 1) & (buffers.length - 1);

		// System.out.println("Fill buffer " + id);

		GeneratedBuffer buffer = buffers[id];
		/*
		 * The buffer can't be locked because a call to getNext() by a single consumer
		 * entails unlocking the previously used buffer.
		 */
		if (buffer.isLocked())
			throw new IllegalStateException ();

		/* Schedule N worker threads to fill this buffer. */
		buffer.setLatch (workers.length);

		/* Unblock all workers */
		next = (next + 1) & (buffers.length - 1);
		count++;
	}

	public void createPositionsList (int inputTupleSize) {
    	int i;
    	int startPos = 0;
    	int incrementStep = (bufferSize % numberOfThreads == 0)? bufferSize / numberOfThreads : ((int) (bufferSize / numberOfThreads / inputTupleSize) * inputTupleSize);
    	int endPos = 0;

    	ArrayList<Integer> threadList;
    	positionsList = new ArrayList<List<Integer>>(numberOfThreads);
    	for (i = 0; i < numberOfThreads; i++) {
    		threadList = new ArrayList<Integer>();
    		threadList.add(startPos);
    		endPos += incrementStep;
    		if (i == (numberOfThreads - 1) && endPos != bufferSize)
    			endPos = bufferSize;
    		threadList.add(endPos - 1);
    		startPos = endPos;
    		positionsList.add(threadList);
    	}
	}
}
