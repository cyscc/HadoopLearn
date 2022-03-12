package com.cyss.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss.domain
 * @Author: cyss
 * @CreatTime: 2022-03-11 19:36
 * @Description: 用来封装map结束后的value数据
 */

// 实现接口 Writable 实现序列化与反序列化
public class CovidCountBean implements WritableComparable<CovidCountBean> {

    private long cases;
    private long deaths;

    public CovidCountBean() {
    }

    public CovidCountBean(long cases, long deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }

    /**
     *  自己封装一个set方法与  Text  的set方法对应
     * @param cases 感染人数
     * @param deaths 死亡人数
     */
    public void set(long cases, long deaths){
        this.cases = cases;
        this.deaths = deaths;
    }

    public long getCases() {
        return cases;
    }

    public void setCases(long cases) {
        this.cases = cases;
    }

    public long getDeaths() {
        return deaths;
    }

    public void setDeaths(long deaths) {
        this.deaths = deaths;
    }

    @Override
    public String toString() {
        return cases + "\t" + deaths;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(cases);
        dataOutput.writeLong(deaths);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.cases = dataInput.readLong();
        this.deaths = dataInput.readLong();
    }

    @Override
    public int compareTo(CovidCountBean o) {
        return this.cases > o.getCases() ? -1 : (this.cases < o.getCases() ? 1 : 0);
    }
}
