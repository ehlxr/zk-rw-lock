package io.github.ehlxr.zkrwlock.controller;


import io.github.ehlxr.zkrwlock.lockv2.ZkLock;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SuppressWarnings("all")
public class Controller {

    String lockName = "test";
    /**
     * ---------------非公平锁，重制命令每次执行都会优先于读取命令
     */
    @GetMapping("/test03")
    public void test03() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            new Thread(()->{
                ZkLock lock = new ZkLock(lockName, ZkLock.ReadWriteType.READ);

                try {
                    lock.lock();
                    Thread.sleep(1000);
                    System.out.println("读请求，耗时50毫秒");
                }catch (Exception e){

                }finally {
                    lock.unLock();
                }

            }).start();
        }
    }
    @GetMapping("/test04")
    public void test04() throws Exception {
        ZkLock lock = new ZkLock(lockName, ZkLock.ReadWriteType.WRITE);

        lock.lock();
        System.out.println("写请求");
        Thread.sleep(2000);
        lock.unLock();
    }


    @GetMapping("/test05")
    public void test05(){


    }
}
