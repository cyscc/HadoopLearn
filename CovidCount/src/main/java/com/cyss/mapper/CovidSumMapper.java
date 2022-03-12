package com.cyss.mapper;

import com.cyss.domain.CovidCountBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss.mapper
 * @Author: cyss
 * @CreatTime: 2022-03-11 19:53
 * @Description: map阶段实现 <key, value>
 */
public class CovidSumMapper extends Mapper<LongWritable, Text, Text, CovidCountBean> {

    private final Text outKey = new Text();
    private final CovidCountBean outValue = new CovidCountBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        outKey.set(fields[2]);
        outValue.set(Long.parseLong(fields[fields.length - 2]), Long.parseLong(fields[fields.length - 1]));

        context.write(outKey, outValue);

    }
}
