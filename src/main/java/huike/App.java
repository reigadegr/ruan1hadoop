package huike;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, URISyntaxException {
        App app = new App();
        app.putFileToHdfs();
    }
    public FileSystem getHdfs() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://192.168.100.101:9000");
        FileSystem hdfs = FileSystem.get(conf);
        System.out.println(hdfs);
        return hdfs;
    }

    public void putFileToHdfs() throws IOException, URISyntaxException {
        //1，构建连接
        FileSystem hdfs = getHdfs();

        System.out.println("hdfs = " + hdfs);
        //2,构建本地
        Path localFilePath = new Path("file:///D:\\bin.txt");
        //3,构建HDFS存储路径
        Path hdfsFilePath = new Path("/files/aa.txt");
        //第二个参数表示是否递归
        hdfs.delete(hdfsFilePath, true);
        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
        hdfs.close();
    }
}
