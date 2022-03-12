package com.cyss.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss.mapreduce
 * @Author: cyss
 * @CreatTime: 2022-03-10 16:37
 * @Description:
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text outKey = new Text();
    private final static LongWritable outValue = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+");   //正则表达式，表示空白符，+表示任意多个
        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}
