package com.eyesmoons.connect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * 使用多线程创建连接
 */
public class HBaseConnectSingleton {
    public static void main(String[] args) throws IOException {
        // 1. 创建配置对象
        Configuration conf = new Configuration();
        // 2. 添加配置参数
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
        // 3. 创建 hbase 的连接
        // 默认使用同步连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 可以使用异步连接
        // 主要影响后续的 DML 操作
        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);
        // 4. 使用连接
        System.out.println(connection);
        // 5. 关闭连接
        connection.close();
    }
}