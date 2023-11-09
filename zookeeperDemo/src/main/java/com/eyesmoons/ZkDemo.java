package com.eyesmoons;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @created by shengyu on 2023/10/29 18:05
 */
@Slf4j
public class ZkDemo {

    private static final String connectString = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private static final int sessionTimeout = 2000;
    private static ZooKeeper zkClient = null;

    // 初始化zk客户端
    public static void init() throws Exception {

        zkClient = new ZooKeeper(connectString, sessionTimeout, event -> {

            // 收到事件通知后的回调函数（用户的业务逻辑）
            System.out.println(event.getType() + "--" + event.getPath());

            // 再次启动监听
            try {
                zkClient.getChildren("/", true);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
    }

    /**
     * 创建节点
     * @param path 要创建的节点的路径
     * @param data 节点数据
     * @param permit 节点权限
     * @param createMode 节点类型
     */
    public static void create(String path, String data, List<ACL> permit, CreateMode createMode) throws Exception {
        String nodeCreated = zkClient.create(path, data.getBytes(), permit, createMode);
        log.info(nodeCreated);
    }

    /**
     * 判断节点是否存在
     * @param path 节点路径
     */
    public static void exist(String path) throws Exception {
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            log.warn("节点[{}]不存在", path);
        } else {
            log.info("节点[{}]信息：{}", path, stat);
        }
    }

    /**
     * 查看节点的值
     * @param path 节点路径
     */
    public static void getData(String path) throws InterruptedException, KeeperException {
        byte[] data = zkClient.getData(path, false, null);
        log.info("节点[{}]的值为：{}", path, new String(data));
    }

    public static void main(String[] args) throws Exception {
        init();
        // 创建节点/demo，并设置值：hello zookeeper
        // create("/demo", "hello zookeeper", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 判断节点是否存在
        // exist("/demo");
        getData("/demo");
    }
}