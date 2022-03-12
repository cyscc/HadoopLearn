package com.cyss.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss.mapreduce
 * @Author: cyss
 * @CreatTime: 2022-03-10 16:54
 * @Description:
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        outValue.set(count);
        context.write(key, outValue);
    }
}
