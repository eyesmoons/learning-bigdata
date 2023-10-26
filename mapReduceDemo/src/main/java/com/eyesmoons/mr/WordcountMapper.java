package com.eyesmoons.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String line = value.toString();
        // 用空格切分
        String[] words = line.split(" ");
        // 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}