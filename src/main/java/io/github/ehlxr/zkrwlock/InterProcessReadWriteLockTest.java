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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryOneTime;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * @author ehlxr
 * @since 2021-02-03 22:42.
 */
public class InterProcessReadWriteLockTest {
    private static final CuratorFramework ZK_CLIENT;

    static {
        ZK_CLIENT = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(2000000)
                .retryPolicy(new RetryOneTime(10000))
                .namespace("lock")
                .build();

        ZK_CLIENT.start();

        if (ZK_CLIENT.getState() == CuratorFrameworkState.STARTED) {
            System.out.println("启动成功");
        }
    }

    public static void main(String[] args) throws Exception {
        InterProcessReadWriteLockTest interProcessReadWriteLockTest = new InterProcessReadWriteLockTest();

        interProcessReadWriteLockTest.reentrantReadLockTest();
        interProcessReadWriteLockTest.wwTest();
        interProcessReadWriteLockTest.rwLockTest();
    }

    public void reentrantReadLockTest() {
        int num = 2;
        // CountDownLatch latch = new CountDownLatch(num);

        IntStream.range(0, num).forEach(i -> {
            // 创建共享可重入读锁
            InterProcessLock readLock = new InterProcessReadWriteLock(ZK_CLIENT, "/test").readLock();

            try {
                // 获取锁对象
                readLock.acquire();
                System.out.println(i + "获取读锁===============");
                // 测试锁重入
                readLock.acquire();
                System.out.println(i + "再次获取读锁===============");
                Thread.sleep(2000);
                readLock.release();
                System.out.println(i + "释放读锁===============");
                readLock.release();
                System.out.println(i + "再次释放读锁===============");

                // latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // latch.await();
    }

    public void rwLockTest() throws Exception {
        /*
         * 读线程不互斥
         */
        int num = 50;
        CyclicBarrier barrier = new CyclicBarrier(num);

        ExecutorService pool = Executors.newFixedThreadPool(num);
        CountDownLatch latch = new CountDownLatch(num);

        /*
         * "+ 开始读请求。。。。" 与 "= 读请求结束。。。。" 交叉出现
         */
        IntStream.range(0, num).forEach(i -> pool.execute(() -> {
            InterProcessMutex lock = new InterProcessReadWriteLock(ZK_CLIENT, "/test").readLock();
            try {
                System.out.println("> 读请求就绪。。。。" + i + " " + Thread.currentThread().getName());
                barrier.await();

                lock.acquire();
                System.out.println("+ 开始读请求。。。。" + i + " " + Thread.currentThread().getName());

                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("= 读请求结束。。。。" + i + " " + Thread.currentThread().getName());

                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                latch.countDown();
            }
        }));

        Thread.sleep(10);
        InterProcessMutex lock = new InterProcessReadWriteLock(ZK_CLIENT, "/test").writeLock();
        try {
            lock.acquire();
            System.out.println("\n开始写请求。。。。");

            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("写请求结束。。。。\n");
            lock.release();
        }

        latch.await();
        pool.shutdown();
    }

    public void wwTest() {
        int num = 5;
        // CountDownLatch latch = new CountDownLatch(num);
        CyclicBarrier barrier = new CyclicBarrier(num);

        /*
         * "+ 开始写请求。。。。" 与 "= 写请求结束。。。。" 成对出现
         */
        IntStream.range(0, num).parallel().forEach(i -> {
            InterProcessMutex lock = new InterProcessReadWriteLock(ZK_CLIENT, "/test").writeLock();
            try {
                System.out.println("> 写请求就绪。。。。" + i + " " + Thread.currentThread().getName());
                barrier.await();

                lock.acquire();
                System.out.println("\n+ 开始写请求。。。。" + i + " " + Thread.currentThread().getName());

                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("= 写请求结束。。。。" + i + " " + Thread.currentThread().getName());

                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // latch.countDown();
            }
        });

        // latch.await();
    }
}
