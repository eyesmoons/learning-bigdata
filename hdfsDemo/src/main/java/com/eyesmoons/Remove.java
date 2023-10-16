package com.eyesmoons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @created by shengyu on 2023/10/15 10:17
 * 删除文件或文件夹
 */
public class Remove {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "casey");
        // 2. 执行删除
        fs.delete(new Path("/demo2.txt"), true);
        // 3. 关闭资源
        fs.close();
    }
}