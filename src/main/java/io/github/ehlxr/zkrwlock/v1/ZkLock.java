package io.github.ehlxr.zkrwlock.v1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public abstract class ZkLock implements Closeable {



    private static final String ROOTLOCK = "/lock";
    protected static final String READ_WRITE_LOCK_PATH = "/lock/readWriteLock";

    protected static final String SERVER = "EUREKA01:2181";

    protected static final Integer TIMEOUT= 20000;

    protected static ZooKeeper zooKeeper;

    // 如果断开链接了，就需要全部暂停等待zk锁从新链接成功
    private static final Object reconnectLock = new Object();

    protected String path ;

    Watcher watcher = event -> {
        if (event.getType() == Watcher.Event.EventType.None) {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("重新连接成功!");
                synchronized (reconnectLock){
                    reconnectLock.notifyAll();
                }
            }
        }

    };
    static {
        try {
            zooKeeper = new ZooKeeper(SERVER, TIMEOUT,event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("重新连接成功!");
                    synchronized (reconnectLock){
                        reconnectLock.notifyAll();
                    }
                }
            });

        }catch (Exception e){
            System.out.println("创建zk失败");
        }
    }
    public ZkLock() throws Exception {

        init();
    }

    public void init() throws Exception{
        try {
            // 创建持久节点 /lock
            if (zooKeeper.exists(ROOTLOCK, false) == null){
                zooKeeper.create(ROOTLOCK, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            // 创建持久节点 /lock/unfairLock
            if (zooKeeper.exists(READ_WRITE_LOCK_PATH, false) == null){
                zooKeeper.create(READ_WRITE_LOCK_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }catch (Exception e){
            System.out.println(e);
            if (e instanceof KeeperException.ConnectionLossException){
                // zookeeper 服务端断开连接，等待重新链接
                synchronized (reconnectLock){
                    reconnectLock.wait();
                }

                init();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            zooKeeper.delete(path, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    protected void cirLock(String path, List<String> list, int index) throws Exception {
        // 监听上一个读锁
        String lastPath = list.get(index - 1);
        Stat stat = zooKeeper.exists(READ_WRITE_LOCK_PATH + "/" + lastPath, event -> {
            //  KeeperState  DisConnected Exipred 发生，临时节点可能也会被删除
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                synchronized (this) {
                    this.notify();
                }
            }
            if (event.getState() == Watcher.Event.KeeperState.Disconnected ||
            event.getState() == Watcher.Event.KeeperState.Expired){
                System.out.println("掉线了，重新链接");
                try {
                    zooKeeper = new ZooKeeper(SERVER, TIMEOUT,watcher);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        if (stat == null) {
            // 上一个节点消失了，再次重新获取锁
            attemptLock(path);
        } else {
            // 阻塞，等待锁释放
            synchronized (this) {
                this.wait();
            }
            attemptLock(path);
        }
    }



    protected abstract void attemptLock(String path) throws Exception;


}
