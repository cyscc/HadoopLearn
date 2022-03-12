package com.cyss.reducer;

import com.cyss.domain.CovidCountBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss.reducer
 * @Author: cyss
 * @CreatTime: 2022-03-11 20:03
 * @Description: reduce合并阶段
 */
public class CovidSumReducer extends Reducer<Text, CovidCountBean, Text, CovidCountBean> {

    private final CovidCountBean outValue = new CovidCountBean();

    @Override
    protected void reduce(Text key, Iterable<CovidCountBean> values, Reducer<Text, CovidCountBean, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {

        long totalCases = 0;
        long totalDeaths = 0;

        for (CovidCountBean value : values) {
            totalCases += value.getCases();
            totalDeaths += value.getDeaths();
        }
        outValue.set(totalCases, totalDeaths);

        context.write(key, outValue);
    }
}
