package com.eyesmoons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @created by shengyu on 2023/10/15 10:13
 * 查看文件
 */
public class Cat {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "casey");
        // 2. 获取文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            String[] hosts = new String[0];
            for (BlockLocation blockLocation : blockLocations) {
                // 获取存储块的主机节点
                hosts = blockLocation.getHosts();
            }
            System.out.printf("文件名：%s，文件路径：%s，文件大小：%sBytes，权限：%s，分组：%s，存储节点：%s%n",
                    status.getPath().getName(),
                    status.getPath().toString(),
                    status.getLen(),
                    status.getPermission(),
                    status.getGroup(),
                    Arrays.toString(hosts));
        }
        // 3. 关闭资源
        fs.close();
    }
}