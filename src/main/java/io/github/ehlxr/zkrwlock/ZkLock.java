package io.github.ehlxr.zkrwlock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.List;
import java.util.stream.Collectors;

/**
 * zk 实现读写锁
 * 实现效果为： 在 lock 节点下创建自定义锁资源，如：lock_01，lock_02 等
 * 锁资源下为有序临时节点，分为读节点和写节点，例如：read_00001，write_00001
 * 获取读锁的方式为，锁资源下没有写节点，如果有则监听最后一个，读锁之间不会相互竞争
 * 获取写锁的方式也是写锁下没有最后一个节点，并且当前有读锁的时候需要监听当前读锁的结束,写锁之间会相互竞争
 *
 * @author ehlxr
 */
public class ZkLock {
    private static final String ROOTLOCK = "lock";
    protected static final String SERVER = "localhost:2181";
    protected static final Integer TIMEOUT = 2000000;
    protected static final CuratorFramework ZK_CLIENT;
    private final String name;
    private final ReadWriteType readWriteType;
    public String path;
    /**
     * 是否可以获取写锁的标志位，获取写锁的条件是
     * 处于写锁的第一个，并且当前没有读锁正在读取写锁的第一个可以通过有序数组排序，没有读锁则得通过监听最老的读锁释放之后，修改这个值
     * 这个标志为同样可以监听第二个写锁监听结束后变成第一个写锁的情况.
     * 判断是否可以获得写锁的标志就是要么 是 写锁的第一个要么就是上一个监听的回掉生效了
     */
    private Boolean shouldWrite = false;

    static {
        ZK_CLIENT = CuratorFrameworkFactory.builder()
                .connectString(SERVER)
                .sessionTimeoutMs(TIMEOUT)
                .retryPolicy(new RetryOneTime(10000))
                // 命名空间，用该客户端操作的东西都在该节点之下
                .namespace(ROOTLOCK)
                .build();

        ZK_CLIENT.start();

        if (ZK_CLIENT.getState() == CuratorFrameworkState.STARTED) {
            System.out.println("启动成功");
        }
    }

    public ZkLock(String name, ReadWriteType readWriteType) {
        this.name = name;
        this.readWriteType = readWriteType;

        try {
            if (ZK_CLIENT.checkExists().forPath("/" + name) == null) {
                ZK_CLIENT.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath("/" + name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void lock() throws Exception {
        path = ZK_CLIENT.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/" + name + "/" + readWriteType.type);
        attemptLock(path);
    }

    public void unLock() {
        try {
            ZK_CLIENT.delete()
                    .deletingChildrenIfNeeded()
                    .forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void attemptLock(String path) throws Exception {
        GetChildrenBuilder children = ZK_CLIENT.getChildren();
        List<String> list = children.forPath(getPath());

        List<String> writeList = list.stream()
                .filter(data -> data.contains(ReadWriteType.WRITE.type))
                .sorted(String::compareTo)
                .collect(Collectors.toList());

        List<String> readList = list.stream()
                .filter(data -> data.contains(ReadWriteType.READ.type))
                .sorted(String::compareTo)
                .collect(Collectors.toList());

        if (readWriteType == ReadWriteType.READ) {
            // 读锁判断最后一个写锁没有了就可以获得锁了
            if (writeList.size() == 0) {
                // 我是读锁，并且没有写锁，直接获得
                // return;
            } else {
                // 读锁但是有写锁，监听最后一个写锁
                String lastPath = writeList.get(writeList.size() - 1);
                cirLock(lastPath);
            }
        } else {
            // 写锁，判断自己是不是第一个，如果不是则必须得等到没有
            if (writeList.size() == 1) {
                // 获取到锁,已经没人获取到读锁了
                if (readList.size() == 0 || shouldWrite) {
                    // return;
                } else {
                    String first = readList.get(0);
                    cirLock(first);
                }
            } else {
                String name = path.substring(getPath().length() + 1);
                if (writeList.lastIndexOf(name) == 0) {
                    // 获取到锁
                    if (readList.size() == 0) {
                        // return;
                    } else {
                        String first = readList.get(0);
                        cirLock(first);
                    }
                } else {
                    // 只需要监听前一个写锁的释放即可
                    String lastPath = writeList.get(writeList.lastIndexOf(name) - 1);
                    cirLock(lastPath);
                }
            }
        }
        // 没有写锁，全部都不阻塞
    }

    protected void cirLock(String lastPath) throws Exception {
        // 获得上一个锁对象
        NodeCache nodeCache = new NodeCache(ZK_CLIENT, getPath() + "/" + lastPath);

        nodeCache.start();

        nodeCache.getListenable().addListener(() -> {
            ChildData currentData = nodeCache.getCurrentData();
            if (currentData == null) {
                synchronized (this) {
                    shouldWrite = true;
                    notifyAll();

                }
            }
        });

        synchronized (this) {
            wait(1000);
        }

        attemptLock(path);
    }

    public enum ReadWriteType {
        /**
         * 锁类型
         */
        READ("read_"),
        WRITE("write_");
        private final String type;

        ReadWriteType(String type) {
            this.type = type;
        }
    }

    private String getPath() {
        return "/" + name;
    }
}
