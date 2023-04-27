package com.cnso.flinkcdc.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Create by zhengtianhao 2023-04-23 0023 19:40:02
 */
public class PoolUtils {

    public static ExecutorService threadPool;

    public static ExecutorService getPool(){
        threadPool = Executors.newFixedThreadPool(10);
        return threadPool;
    }
}
