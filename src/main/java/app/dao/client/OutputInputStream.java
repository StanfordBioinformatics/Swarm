package app.dao.client;

import app.AppLogging;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * While the stream is marked as open, it will wait for input
 * data. To interrupt blocking and close, a -1 value should
 * be sent to the readQueue.
 */
public class OutputInputStream extends InputStream {
    private Logger log = AppLogging.getLogger(OutputInputStream.class);
    private BlockingQueue<Integer> readQueue = new LinkedBlockingQueue<Integer>();

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    @Override
    public int read() throws IOException {
        if (!readQueue.isEmpty() || !isClosed()) {
            try {
                //log.debug("Waiting for integer from readQueue");
                Integer i = readQueue.take();
                if (i == -1) {
                    log.debug("Received queue signal to end stream");
                    return -1;
                }
                //log.debug("Finished reading integer " + i + " from readQueue");
                // add(int) already truncated negative sign extensions
                return i;
            } catch (InterruptedException e) {
                e.printStackTrace();
                if (!isClosed()) {
                    throw new IOException(e);
                }
            }
        }
        return -1;
    }

    protected void add(int i) throws InterruptedException {
        // We're using (int)(-1) value to wake up blocking readQueue when blocked
        // at end of stream, so we need to truncate input -1 values to bytes explicitly.
        // Is probably best to just do this for all negative int values.
        if (i < 0) {
            int newVal = 0x000000FF & i;
            //log.debug("Old value: %08X, newVal: %08X\n", i, newVal);
            i = newVal;
        }
        this.readQueue.put(i);
    }

    @Override
    public void close() {
        this.setClosed();
        try {
            this.readQueue.put(-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected synchronized boolean isClosed() {
        return this.isClosed.get();
    }

    private synchronized void setClosed() {
        this.isClosed.set(true);
    }
}
