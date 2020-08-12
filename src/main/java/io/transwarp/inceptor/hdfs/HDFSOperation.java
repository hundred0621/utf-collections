package io.transwarp.inceptor.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HDFSOperation {
    private String hdfsSite;
    private String coreSite;
    private String krb5conf;
    private String fshdfs;
    private static String user;
    private static String keytab;
    private Configuration conf;
    private FileSystem fs;

    public HDFSOperation(){
        Properties pps = new Properties();
        try{
            InputStream in =  HDFSOperation.class.getClassLoader().getResourceAsStream("hdfs.properties");
            pps.load(in);
            hdfsSite = pps.getProperty("hdfs-site", "/etc/hadoop/conf/hdfs-site.xml");
            coreSite = pps.getProperty("core-site", "/etc/hadoop/conf/core-site.xml");
            krb5conf = pps.getProperty("krb5", "/etc/hadoop/conf/krb5.conf");
            keytab = pps.getProperty("keytab", "/etc/hadoop/conf/hdfs.keytab");
            fshdfs = pps.getProperty("fshdfs", "org.apache.hadoop.hdfs.DistributedFileSystem");
            user = pps.getProperty("user" ,"hdfs@CH11409.TDH");

            conf = getHDFSConf();
            fs = FileSystem.get(conf);
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public Configuration getHDFSConf() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path(hdfsSite));
        conf.addResource(new Path(coreSite));
        conf.set("fs.hdfs.impl",fshdfs);
        conf.setBoolean("dfs.support.append", true);
        System.setProperty("java.security.krb5.conf", krb5conf);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(user, keytab);
        return conf;
    }


    public List<FileStatus> getAllFileList(String path){
        List<FileStatus> fileList=new ArrayList<FileStatus>();
        try{
            FileStatus[] files = fs.listStatus(new Path(path));
            for(FileStatus file: files) {
                if(file.isFile()){
                    fileList.add(file);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return fileList;
    }

    /**
     * list directory
     * @param path
     * @throws IOException
     */
    public FileStatus[] listDir(String path){
        FileStatus [] files = new FileStatus[]{};
        try {
            files = fs.listStatus(new Path(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }



    public byte[] download(Path path) throws IOException {
        FileSystem fs = FileSystem.get(getHDFSConf());
        if(fs.exists(path)){
            FSDataInputStream in = fs.open(path);
            FileStatus stat = fs.getFileStatus(path);
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            in.readFully(0, buffer);
            in.close();
            fs.close();
            return buffer;
        }else{
            return null;
        }


    }

    public static void readFile(Configuration conf, String file) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(file),conf);
        FSDataInputStream hdfsIS = fs.open(new Path(file));
        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsIS.read(ioBuffer);
        while (readLen != -1) {
            System.out.write(ioBuffer, 0, readLen);
            readLen = hdfsIS.read(ioBuffer);
        }
        hdfsIS.close();
        fs.close();
    }


    public static void writeFile(Configuration conf, String file, String str) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        FSDataOutputStream hdfsOS = null;
//        System.out.println("路径是否存在:" + fs.exists(new Path(file)));
        if (!fs.exists(new Path(file))){
            hdfsOS = fs.create(new Path(file));
        }else {
            hdfsOS = fs.append(new Path(file));
        }
//        hdfsOS.writeChars(URLDecoder.decode(str, "UTF-8"));
        hdfsOS.write(str.getBytes(), 0, str.getBytes().length);
        hdfsOS.close();
        fs.close();
    }

    /**
     * mkdir directory
     * @param args
     * @throws IOException
     */
    public void mkdirDir(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Mkdir method requires dirname!");
            System.exit(1);
        }
        String dst = args[1];
        String user = args[2];
        FileSystem fs = FileSystem.get(URI.create(dst), getHDFSConf());
        Path path = new Path(dst);
        if(fs.exists(path)) {
            throw new IOException("The directory has existed in cureent environment!");
        }
        fs.mkdirs(path);
    }

    /**
     * Delete file
     * @param args
     * @throws Exception
     */
    public void deleteFile(String[] args) throws Exception {
        if(args.length<2){
            System.out.println("Delete file method requires file name!");
            System.exit(1);
        }
        String fileName = args[1];
        String user = args[2];
        FileSystem fs = FileSystem.get(URI.create(fileName), getHDFSConf());
        Path path = new Path(fileName);
        if(!fs.exists(path)) {
            throw new IOException("The directory doesn't exist in cureent environment!");
        }
        boolean isDeleted = fs.delete(path, false);
        if(isDeleted) {
            System.out.println("delete file success");
        }else{
            System.out.println("delete file fail");
            System.exit(1);
        }
    }


    public void downloadHDFSFile(String hdfsFile, String localFile){
        FSDataInputStream fsDataInputStream = null;
        FileOutputStream outputStream = null;
        try {
            Path path = new Path(hdfsFile);
            outputStream = new FileOutputStream(new File(localFile));
            fsDataInputStream = this.fs.open(path);
            IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fsDataInputStream != null){
                IOUtils.closeStream(fsDataInputStream);
            }
            if(outputStream != null){
                IOUtils.closeStream(outputStream);
            }
        }
    }

    public byte[] readHDFSFile(String filePath){
        byte [] bytes = null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        FSDataInputStream fsDataInputStream = null;
        try {
            Path path = new Path(filePath);
            fsDataInputStream = this.fs.open(path);
            IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, false);
            bytes = outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fsDataInputStream != null){
                IOUtils.closeStream(fsDataInputStream);
            }
            IOUtils.closeStream(outputStream);
        }
        return bytes;

    }

    /**
     * upload local file to hdfs directory
     * @param
     * @throws IOException
     */
    public void uploadLocalFileHDFS(String srcFilePath, String dstFilePath) throws IOException {
        FSDataOutputStream outputStream = null;
        FileInputStream fileInputStream = null;
        try {
            Path path = new Path(dstFilePath);
            outputStream = this.fs.create(path);
            fileInputStream = new FileInputStream(new File(srcFilePath));            //输入流、输出流、缓冲区大小、是否关闭数据流，如果为false就在 finally里关闭
            IOUtils.copyBytes(fileInputStream, outputStream, 4096, false);
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileInputStream != null){
                IOUtils.closeStream(fileInputStream);
            }
            if(outputStream != null){
                IOUtils.closeStream(outputStream);
            }
        }
    }

    public void UploadBytesHDFS(String path, byte[] data){
        FSDataOutputStream out = null;
        try {
            Path dst = new Path(path);
            out = fs.create(dst);
            out.write(data);
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(out != null){
                IOUtils.closeStream(out);
            }
        }
    }

}

