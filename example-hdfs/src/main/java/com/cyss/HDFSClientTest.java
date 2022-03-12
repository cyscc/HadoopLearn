package com.cyss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @ProjectName: HadoopLearn
 * @PackageName: com.cyss
 * @Author: cyss
 * @CreatTime: 2022-03-03 16:01
 * @Description:
 */
public class HDFSClientTest {

    private Configuration configuration = null;
    private FileSystem fileSystem = null;

    @Before
    public void connect2HDFS() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        fileSystem = FileSystem.get(configuration);
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        if(!fileSystem.exists(new Path("/cyss"))){
            fileSystem.mkdirs(new Path("/cyss"));
        }
    }

    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void put() throws IOException {
        Path src = new Path("E:\\Program Data New\\HadoopLearn\\example-hdfs\\Hello.txt");
        Path dst = new Path("/cyss");
        fileSystem.copyFromLocalFile(src, dst);
    }


    @After
    public void close(){
        if(fileSystem != null){
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
