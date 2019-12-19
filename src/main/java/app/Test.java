package app;

import app.dao.client.AthenaClient;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.ResultSet;
import com.amazonaws.services.athena.model.Row;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Test {

    public static void main(String[] args) {

        //ThreadPoolExecutor threadPool = new ThreadPoolExecutor(10, 10, 1000, TimeUnit.SECONDS, new ConcurrentLinkedQueue<Runnable>());

        //ExecutorService es = Executors.newFixedThreadPool(2);
        ExecutorService es = Executors.newSingleThreadExecutor();
        Runnable r1 = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("In Thread 1");
            }
        };
        Runnable r2 = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("In Thread 2");
            }
        };
        Runnable r3 = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("In Thread 3");
            }
        };
        es.submit(r1);
        es.submit(r2);
        es.submit(r3);

    }


    public static void test1() {
        String awsCredentialFilePath = "aws.properties";
        AthenaClient athenaClient = new AthenaClient("swarm", awsCredentialFilePath);
        GetQueryResultsResult getQueryResultsResult = athenaClient.executeQueryToResultSet("select * from swarm.thousandorig_half2");
        ResultSet rs = getQueryResultsResult.getResultSet();
        StringBuilder sb = new StringBuilder();
        for (Row row : rs.getRows()) {
            sb.append(row.toString());
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    public static void timings(String[] args) throws InterruptedException {
        long duration;
        LinkedList<Integer> linkedList = new LinkedList<>();
        int listSize = 1000000; // 1M
        int testIter = 5000; // 5K
        for (int i = 0; i < listSize; i++) {
            linkedList.add(i);
        }
        LinkedListIteratorCache<Integer> linkedListCached = new LinkedListIteratorCache<>();
        for (int i = 0; i < listSize; i++) {
            linkedListCached.add(i);
        }
        long seed = 1;//System.currentTimeMillis();
        Random rand = new Random(seed);
        int[] randIndexes = new int[testIter];
        for (int i = 0; i < testIter; i++) {
            randIndexes[i] = rand.nextInt(listSize);
        }

        int[] llic = new int[testIter];
        int[] ll = new int[testIter];

        // SEQUENTIAL ACCESS

        System.out.println("SINGLE-THREADED SEQUENTIAL ACCESS");
        duration = getSequential(linkedListCached, testIter, llic);
        System.out.printf("LinkedListIteratorCache sequential: %dms\n", duration);
        duration = getSequential(linkedList, testIter, ll);
        System.out.printf("LinkedList sequential: %dms\n", duration);

        // Assert equal results
        for (int i = 0; i < testIter; i++) {
            assert (ll[i] == llic[i]);
        }

        System.gc();
        System.out.println("SINGLE-THREADED RANDOM ACCESS");
        linkedListCached.resetIterator();
        // RANDOM ACCESS
        duration = getRandom(linkedListCached, testIter, llic, randIndexes);
        System.out.printf("LinkedListIteratorCache random: %dms, iterator uses: %d, regular get uses: %d\n",
                duration, linkedListCached.getIteratorUses(), linkedListCached.getRegularGetUses());
        duration = getRandom(linkedList, testIter, ll, randIndexes);
        System.out.printf("LinkedList random: %dms\n", duration);

        // Assert equal results
        for (int i = 0; i < testIter; i++) {
            assert(ll[i] == llic[i]);
        }

        // THREADED RANDOM
        linkedListCached.resetIterator();
        System.gc();
        int[] llic1 = llic;
        int[] llic2 = new int[testIter];
        int[] ll1 = ll;
        int[] ll2 = new int[testIter];
        System.out.println("MULTI-THREADED RANDOM ACCESS");
        Runnable r1 = new Runnable() {
            @Override
            public void run() {
                long duration = getRandom(linkedListCached, testIter, llic1, randIndexes);
                System.out.printf("LinkedListIteratorCache random: %dms, iterator uses: %d, regular get uses: %d\n",
                        duration, linkedListCached.getIteratorUses(), linkedListCached.getRegularGetUses());
            }
        };
        Runnable r2 = new Runnable() {
            @Override
            public void run() {
                long duration = getRandom(linkedListCached, testIter, llic2, randIndexes);
                System.out.printf("LinkedListIteratorCache random: %dms, iterator uses: %d, regular get uses: %d\n",
                        duration, linkedListCached.getIteratorUses(), linkedListCached.getRegularGetUses());
            }
        };
        Runnable r3 = new Runnable() {
            @Override
            public void run() {
                long duration = getRandom(linkedList, testIter, ll1, randIndexes);
                System.out.printf("LinkedList random: %dms\n", duration);
            }
        };
        Runnable r4 = new Runnable() {
            @Override
            public void run() {
                long duration = getRandom(linkedList, testIter, ll2, randIndexes);
                System.out.printf("LinkedList random: %dms\n", duration);
            }
        };
        Thread t1 = new Thread(r1);
        Thread t2 = new Thread(r2);
        Thread t3 = new Thread(r3);
        Thread t4 = new Thread(r4);
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        // Assert equal results
        for (int i = 0; i < testIter; i++) {
            assert(ll1[i] == llic1[i]);
            assert(ll1[i] == llic2[i]);
            assert(ll2[i] == llic1[i]);
            assert(ll2[i] == llic2[i]);
        }
    }

    public static long getSequential(List<Integer> list, int testIter, int[] output) {
        long start, end;
        start = System.currentTimeMillis();
        for (int i = 0; i < testIter; i++) {
            int val = list.get(i);
            output[i] = val;
        }
        end = System.currentTimeMillis();
        return end - start;
    }

    public static long getRandom(List<Integer> list, int testIter,
                                 int[] output, int[] randIndexes) {
        long start, end;
        start = System.currentTimeMillis();
        for (int i = 0; i < testIter; i++) {
            if (i % 1000 == 0) {
                System.out.printf("%d ", i);
            }
            int val = list.get(randIndexes[i]);
            output[i] = val;
        }
        System.out.println();
        end = System.currentTimeMillis();
        return end - start;
    }

    private static boolean isDebug = true;

    private static void debugf(String fmt, Object... args) {
        if (isDebug) {
            System.out.printf(fmt, args);
        }
    }

    private static void debugln(String line) {
        if (isDebug) {
            System.out.println(line);
        }
    }

    public static class LinkedListIteratorCache<E> extends LinkedList<E> {
        private ListIterator<E> iterator = null;
        private int iteratorIndex = -1;
        private int iteratorUses = 0;
        private int regularGetUses = 0;
        private final AtomicBoolean lock = new AtomicBoolean(false);

        public void resetIterator() {
            synchronized (lock) {
                iterator = null;
                iteratorIndex = -1;
                iteratorUses = 0;
                regularGetUses = 0;
            }
        }

        public int getIteratorUses() {
            return iteratorUses;
        }

        public int getRegularGetUses() {
            return regularGetUses;
        }

        private int abs(int val) {
            if (val < 0) {
                return 0 - val;
            }
            return val;
        }

        private boolean shouldUseCache(int index) {
            int iteratorDistance = abs(iteratorIndex - index);
//            debugf("iteratorIndex: %d, index: %d, iteratorDistance: %d\n",
//                    iteratorIndex, index, iteratorDistance);
            if (iteratorDistance < index) {
                // iterator is closer than starting at head
                return true;
            }
            return false;
        }

        @Override
        public E get(int index) {
            E val = null;
            if (lock.compareAndSet(false, true)) {
                // was unlocked
                if (this.iterator != null) {
                    if (shouldUseCache(index)) {
                        /*if (index == iteratorIndex) {
                            val = iterator.next();
                            lock.set(false);
                            iteratorIndex++;
                            iteratorUses++;
                        } else */
                        if (iteratorIndex < index) {
                            // move towards tail
                            int stop = index - 1;
                            while (iteratorIndex != stop) {
                                iterator.next();
                                iteratorIndex++;
                            }
                            val = iterator.next();
                            lock.set(false);
                            iteratorUses++;
                        } else if (iteratorIndex > index) {
                            // move towards head
                            int stop = index + 1;
                            while (iteratorIndex != stop) {
                                iterator.previous();
                                iteratorIndex--;
                            }
                            val = iterator.previous();
                            lock.set(false);
                            iteratorUses++;
                        } else /*if (index == iteratorIndex)*/ {
                            val = iterator.next();
                            lock.set(false);
                            iteratorIndex++;
                            iteratorUses++;
                        }
                    } else {
                        // iterator is initialized but not used for this call
                        this.iterator = this.listIterator(index);
                        val = this.iterator.next();
                        lock.set(false);
                        iteratorIndex = index;
                    }
                } else {
                    // iterator was null
                    this.iterator = this.listIterator(index);
                    val = this.iterator.next();
                    lock.set(false);
                    iteratorIndex = index;
                }
                return val;
            } else {
                regularGetUses++;
                return super.get(index);
            }
        }


        //        //@Override
//        public E get1(int index) {
//            E val = null;
//            synchronized (lock) {
//                if (iterator != null) {
//                    if (iteratorIndex == index) {
//                        iteratorIndex++;
//                        iteratorUses++;
//                        return iterator.next();
//                    } else if (iteratorIndex + 1 == index) {
//                        iteratorIndex++;
//                        iteratorUses++;
//                        return iterator.next();
//                    } else if (iteratorIndex - 1 == index) {
//                        iteratorIndex--;
//                        iteratorUses++;
//                        return iterator.previous();
//                    }
//                }
//                iterator = this.listIterator(index);
//                val = iterator.next();
//                iteratorIndex = index + 1;
//                return val;
//            }
//        }
    }
}
