package io.github.ehlxr.zkrwlock.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


/**
 * 写锁优先
 */
@SuppressWarnings("all")
public class ReadWriteLock extends ZkLock {
    private static final String READ_WRITE_NODE = "lock_";

    // 有写锁的时候读锁等待，写锁永远都可以插队
    public static String READ = "read_";
    public static String WRITE = "write_";

    // 当前锁的状态
    private String state;

    public ReadWriteLock(String state) throws Exception {
        this.state = state;


        // 1. 创建临时有序节点
        path = zooKeeper.create(READ_WRITE_LOCK_PATH + "/" + READ_WRITE_NODE + state, "".getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // 2. 获得路径下所有节点

        attemptLock(path);

    }

    @Override
    protected void attemptLock(String path) throws Exception {
        List<String> list = zooKeeper.getChildren(READ_WRITE_LOCK_PATH, false);
        List<String> readList = list.stream().filter(data -> data.contains(READ)).collect(Collectors.toList());
        List<String> writeList = list.stream().filter(data -> data.contains(WRITE)).collect(Collectors.toList());
        Collections.sort(readList);
        Collections.sort(writeList);
        if (READ.equals(state)) {
            // 读锁的获取方式必须是前面没有写锁，并且自己是第一个，否则阻塞
            if (writeList.size() == 0) {

                int index = readList.indexOf(path.substring(READ_WRITE_LOCK_PATH.length() + 1));
                if (index == 0) {
//                    System.out.println("获取到锁对象");
                    return;
                } else {
                    cirLock(path, readList, index);

                }
            } else {
                // 有写锁，需要监听最后一个写锁
                cirLock(path, writeList, writeList.size());

            }
        } else {
            // 写锁的获取方式是前面没有写锁
            int index = writeList.indexOf(path.substring(READ_WRITE_LOCK_PATH.length() + 1));
            if (index == 0) {
//                System.out.println("写锁获取到锁对象");
                return;
            } else {
                // 监听上一个读锁
                cirLock(path, writeList, index);
            }

        }

    }


}
