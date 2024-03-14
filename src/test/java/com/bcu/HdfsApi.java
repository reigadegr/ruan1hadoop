package com.bcu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;


/**
 * HDFS API学习
 */
public class HdfsApi {
    //    @Test
    public FileSystem getHdfs() throws IOException, URISyntaxException {
        /*
          构建一个当前程序的配置对象，用于管理所有的配置：*-default。xml
          这个对象会加载所有的*-default。xml中的默认配置
          然后加载所有的*-size。xml中的自定义的配置，用这些自定义的配置替换默认配置
          如何让当前地址知道HDFS的地址？也就是fs.defaultFS
          方法1：创建一个resource资源目录，将core-size.xml复制粘贴到resources中
         */
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://node-1:9000");
        FileSystem hdfs = FileSystem.get(conf);

        System.out.println(hdfs);

        return hdfs;

    }

    @Test
    public void getClustreInfo() throws IOException, URISyntaxException {
        DistributedFileSystem dfs = (DistributedFileSystem) getHdfs();
//       //获取集群状态信息
        DatanodeInfo[] datanodeStats = dfs.getDataNodeStats();
        for (DatanodeInfo datanodeInfo : datanodeStats) {
            System.out.println(datanodeInfo.getDatanodeReport());
        }
    }

    @Test
    public void createHdfsDir() throws IOException, URISyntaxException {
        //1,获取hdfs连接对象
        FileSystem hdfs = getHdfs();
        //2,构建牡蛎的创建路径对象
        Path path = new Path("/windows");
        if (hdfs.exists(path)) {
            //第二个参数表示是否递归
            hdfs.delete(path, true);
        }
        hdfs.mkdirs(path);
    }

    @Test
    public void putFileToHdfs() throws IOException, URISyntaxException {
        //1，构建连接
//        FileSystem hdfs = getHdfs();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://node-1:9000");
        FileSystem hdfs = FileSystem.get(conf);

        System.out.println("hdfs = " + hdfs);
        //2,构建本地
        Path localFilePath = new Path("file:///D:\\bin.txt");
        //3,构建HDFS存储路径
        Path hdfsFilePath = new Path("/files");
        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
        hdfs.close();
    }

    @Test
    public void downloadFileromHdfs() throws IOException, URISyntaxException {
        FileSystem hdfs = getHdfs();

        System.out.println("hdfs = " + hdfs);

        Path hdfsFilePath = new Path("/files/bin.txt");
        Path localFilePath = new Path("file:///D:\\files\\dn.txt");
        hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
        hdfs.close();
    }

    @Test
    public void listHdfs1() throws IOException, URISyntaxException {
        //1,构件连接
        FileSystem hdfs = getHdfs();
        //2，创建被列举的目录地址对象
        Path path = new Path("/");

        //3，获取目录下文件状态
        FileStatus[] fileStatuses = hdfs.listStatus(path);
        for (FileStatus tmp : fileStatuses) {
            if (hdfs.isDirectory(tmp.getPath())) {
                System.out.println(tmp.getPath() + "是目录");
            } else {
                System.out.println(tmp.getPath() + "是文件");
            }
        }
        //,迭代输出
    }

    @Test
    public void listHdfs2() throws IOException, URISyntaxException {
        //1,构件连接
        FileSystem hdfs = getHdfs();
        RemoteIterator<LocatedFileStatus> fileStatusIterator = hdfs.listFiles(new Path("/"), true);
        while (fileStatusIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusIterator.next();
            System.out.println(fileStatus.getPath());
        }
    }

    @Test
    public void mergeUpload() throws IOException, URISyntaxException {
        //1,构建连接
        FileSystem hdfs = getHdfs();
        //2,构建本地文件系统对象
        LocalFileSystem local=FileSystem.getLocal(new Configuration());

        //3，通过本地文件系统对象构建输入流
        FileStatus[] fileStates = local.listStatus(new Path("D:\\output\\merge"));
        //4，通过hdfs 对象构件输出流
        FSDataOutputStream outputStream =hdfs.create(new Path("/files/shit2"));
        //5，打开没一个文件 获取输入流
        for(FileStatus fileStatus:fileStates){
            FSDataInputStream open=local.open(fileStatus.getPath());
            //6,输入流接入输出流
            IOUtils.copyBytes(open,outputStream,4096);
            IOUtils.closeStream(outputStream);
        }

    }
}
