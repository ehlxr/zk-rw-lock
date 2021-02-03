/*
 * The MIT License (MIT)
 *
 * Copyright © 2020 xrv <xrg@live.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.github.ehlxr.zkrwlock;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * @author ehlxr
 * @since 2021-02-03 20:54.
 */
public class ZkRWLockTest {
    private final String lockName = "test";

    @Test
    public void rwTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        // ExecutorService pool = Executors.newFixedThreadPool(10);
        // ForkJoinPool pool = ForkJoinPool.commonPool();

        IntStream.range(0, 100).parallel().forEachOrdered(i -> {
            // pool.execute(() -> {
            ZkLock lock = new ZkLock(lockName, ZkLock.ReadWriteType.READ);
            try {
                lock.lock();
                Thread.sleep(500);
                System.out.println("读请求" + i);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unLock();
            }
            // });
        });

        Thread.sleep(1000);
        ZkLock lock = new ZkLock(lockName, ZkLock.ReadWriteType.WRITE);
        try {
            lock.lock();
            System.out.println("开始写请求。。。。");

            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("写请求结束。。。。");
            lock.unLock();
        }

        latch.await();
        // pool.shutdown();
    }

    @Test
    public void wwTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        // ExecutorService pool = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);

        IntStream.range(0, 2).parallel().forEach(i -> {
            // pool.execute(() -> {
            ZkLock lock = new ZkLock(lockName, ZkLock.ReadWriteType.WRITE);
            try {
                System.out.println("写请求就绪。。。。" + i);
                barrier.await();

                lock.lock();
                System.out.println("开始写请求。。。。" + i);

                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("写请求结束。。。。" + i);
                latch.countDown();
                lock.unLock();
            }
            // });
        });

        latch.await();
        // pool.shutdown();
    }
}
