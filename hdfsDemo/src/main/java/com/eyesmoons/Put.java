package com.eyesmoons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @created by shengyu on 2023/10/15 10:11
 * 上传文件
 */
public class Put {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "casey");
        // 2. 上传文件
        fs.copyFromLocalFile(new Path("/Users/casey/Data/share/demo2.txt"), new Path("/demo2.txt"));
        // 3. 关闭资源
        fs.close();
    }
}