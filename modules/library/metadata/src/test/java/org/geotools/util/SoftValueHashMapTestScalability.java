package org.geotools.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.Ignore;
import org.junit.Test;

public class SoftValueHashMapTestScalability {
    /**
     * The size of the test sets to be created.
     */
    private static final int SAMPLE_SIZE = 200;
    private static int NUMTHREADS = 512;
    private static int THREAD_CYCLES=(int)1E2;
    private static int WARMUP_CYCLES = (int)1E2;
    private static int TEST_CYCLES = (int)1E2;

    
    SoftValueHashMap<Integer, Integer> cache;
     
    // test get on the same value
    @Test
    //@Ignore
    public void testScalability_1() throws InterruptedException {
        final Random random = getRandom();
        cache = new SoftValueHashMap<Integer, Integer>();
        
        // fill the cache
        for (int i = 0; i < SAMPLE_SIZE*2; i++) {
            final Integer key = random.nextInt(SAMPLE_SIZE);
            final Integer value = random.nextInt(SAMPLE_SIZE);
            cache.put(key, value);
        }
        
        long mean = 0;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        long start;
        long end;
        
        // create many threads that access the same key
        ExecutorService executor = Executors.newFixedThreadPool(NUMTHREADS);
        
        for (int iter = 0; iter < (WARMUP_CYCLES + TEST_CYCLES); iter++) {
            Integer key;
            do {
                key = random.nextInt(SAMPLE_SIZE);            
            }while(!cache.containsKey(key)); // get a key that exist in cache
            
            final CountDownLatch latch = new CountDownLatch(NUMTHREADS);
            //System.out.println("Cycle: "+ iter);
            
            start = System.nanoTime();
            
            for(int i=0; i < NUMTHREADS; i++) {
                Runnable th = new CacheTestThread(i, cache, key, latch);
                executor.execute(th);
            }            

            latch.await();
            end = System.nanoTime() - start;
            
            if(iter > WARMUP_CYCLES - 1)
            {
                if(iter == WARMUP_CYCLES)
                    mean = end;
                else mean += end;
                
                if(end > max)
                    max = end;
                if(end < min)
                    min = end;        
            }
        }
        
        double meanD = mean / (TEST_CYCLES) * 1E-6;
        double maxD = max * 1E-6;
        double minD = min * 1E-6;
        
        System.out.println("mean: "+ meanD);
        System.out.println("max: "+ maxD);
        System.out.println("min: "+ minD);
        
        executor.shutdown();      
        try {
            executor.awaitTermination(60000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    // test sequence of put and get
    //@Test
    @Ignore
    public void testScalability_2() throws InterruptedException {
        final Random random = getRandom();
        cache = new SoftValueHashMap<Integer, Integer>();
       
        long mean = 0;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        long start;
        long end;

        // create many threads that access the same key
        ExecutorService executor = Executors.newFixedThreadPool(NUMTHREADS);        
        
        for (int iter = 0; iter < (WARMUP_CYCLES + TEST_CYCLES); iter++) {
           
            final CountDownLatch latch = new CountDownLatch(NUMTHREADS);
            //System.out.println("Cycle: "+ iter);

            start = System.nanoTime();
            for(int i=0; i < NUMTHREADS; i++) {
                Runnable th = new CacheTestThreadOps(i,cache, random,latch);
                executor.execute(th);
            }  
            
            latch.await();
            end = System.nanoTime() - start;
            
            //System.out.println("Cycle time: "+ end/1E6);
            if(iter > WARMUP_CYCLES - 1)
            {
                if(iter == WARMUP_CYCLES)
                    mean = end;
                else mean += end;
                
                if(end > max)
                    max = end;
                if(end < min)
                    min = end;        
            }     
        }
        
        double meanD = mean / (TEST_CYCLES) * 1E-6;
        double maxD = max * 1E-6;
        double minD = min * 1E-6;
        
        System.out.println("mean: "+ meanD);
        System.out.println("max: "+ maxD);
        System.out.println("min: "+ minD);
        
        executor.shutdown();
        try {
            executor.awaitTermination(60000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }        
    }
    
    // test concurrent use of put, get and iterators
    @Test
    //@Ignore
    public void testScalability_3() throws InterruptedException {
        final Random random = getRandom();
        cache = new SoftValueHashMap<Integer, Integer>();
       
        // create many threads that access the same key
        ExecutorService executor = Executors.newFixedThreadPool(NUMTHREADS);        
        
        for (int iter = 0; iter < (WARMUP_CYCLES + TEST_CYCLES); iter++) {
           
            final CountDownLatch latch = new CountDownLatch(NUMTHREADS);
            //System.out.println("Cycle: "+ iter);

            for(int i=0; i < NUMTHREADS-100; i++) {
                Runnable th = new CacheTestThreadOps(i,cache, random,latch);
                executor.execute(th);
            }
            
            for(int i=0; i < NUMTHREADS+100; i++) {
                Runnable th = new CacheTestThreadIterators(i,cache, random,latch);
                executor.execute(th);
            }
            
            latch.await();     
        }
        
        executor.shutdown();
        try {
            executor.awaitTermination(60000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    
    
    
    
    private Random getRandom() {
        long seed = System.currentTimeMillis() + hashCode();
        Random random = new Random(seed);
        Logger.getLogger(this.getClass().getName()).info("Using Random Seed: " + seed);
        return random;
    }

    private class CacheTestThread implements Runnable {
        private SoftValueHashMap<Integer, Integer> cache;
        private int key;
        private CountDownLatch latch;
        int idx;
        
        public int getIdx() {
            return idx;
        }
        
        public CacheTestThread(int idx, SoftValueHashMap<Integer, Integer> cache, Integer key, CountDownLatch latch) {
            this.cache = cache;
            this.key = key;
            this.idx = idx;
            this.latch = latch;
        }        

        public void run() {
//          System.out.println("JOB "+ idx+ " Started");            
            for(int i = 0; i < THREAD_CYCLES; i++) {
                cache.get(key);
            }
            latch.countDown();
//          System.out.println("JOB "+ idx+ " Terminated");
        }
    }
    
    private class CacheTestThreadOps implements Runnable{

        private SoftValueHashMap<Integer, Integer> cache;
        private Random random;
        private CountDownLatch latch;
        private int idx;
        
        public int getIdx() {
            return idx;
        }
        
        public CacheTestThreadOps(int idx, SoftValueHashMap<Integer, Integer> cache, Random random, CountDownLatch latch) {
            this.cache = cache;
            this.random = random;
            this.latch=latch;
            this.idx=idx;
        }
        
        public void run() {
//            System.out.println("JOB "+ idx+ " Started");
            for(int i=0; i<THREAD_CYCLES; i++) {
                final Integer key   = new Integer(random.nextInt(SAMPLE_SIZE));
                final Integer value = new Integer(random.nextInt(SAMPLE_SIZE));
                if (random.nextBoolean()) {
                    cache.put(key, value);
                } else {
                    cache.get(key);
                }                
            }    
            latch.countDown();
//            System.out.println("JOB "+ idx+ " Terminated");
        } 
    }
    
    private class CacheTestThreadIterators implements Runnable{

        private SoftValueHashMap<Integer, Integer> cache;
        private Random random;
        private CountDownLatch latch;
        private int idx;
        
        public int getIdx() {
            return idx;
        }
        
        public CacheTestThreadIterators(int idx, SoftValueHashMap<Integer, Integer> cache, Random random, CountDownLatch latch) {
            this.cache = cache;
            this.random = random;
            this.latch=latch;
            this.idx=idx;
        }
        
        public void run() {
//            System.out.println("JOB "+ idx+ " Started");
            for(int i=0; i<THREAD_CYCLES; i++) {
                for(Object value : cache.values()) {
                    if(value != null){
                        if(!(value instanceof Integer))
                            System.out.println("" + value.getClass());
                        //assertTrue(value instanceof Integer);
                    }
                }              
            }    
            latch.countDown();
//            System.out.println("JOB "+ idx+ " Terminated");
        } 
    }
}
