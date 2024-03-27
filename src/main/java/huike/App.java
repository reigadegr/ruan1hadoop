package huike;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        //2.读取本地文件
        FileInputStream fileInputStream = new FileInputStream(new File("hdfs://192.168.100.101:9000/opt/aa.txt"));
        //3.找到文件系统，上传到目的地
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream outputStream = fileSystem.create(new Path("/aa.txt"));

        IOUtils.copyBytes(fileInputStream,outputStream, configuration);
        //4.释放资源
        outputStream.close();
        fileSystem.close();
    }
}
