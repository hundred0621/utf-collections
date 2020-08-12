package io.transwarp.inceptor.udf;

import io.transwarp.inceptor.hdfs.HDFSOperation;
import io.transwarp.inceptor.hdfs.HttpfsOperation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Properties;

public class UploadImage extends UDF {
    public static final Logger log = LogManager.getLogger(UploadImage.class);
    Properties pps = new Properties();
    public static void main(String[] args) {
        UploadImage ui = new UploadImage();
        BytesWritable bw = ui.evaluate(new Text("http://ig-ivr.oss-cn-shanghai.aliyuncs.com/301dd7dd8dad4db9039c7ccfadbec1b2.wav"),
                new Text("net_binary~2020-03-07"));
        System.out.println(bw.toString());
//        BytesWritable result = ui.evaluate(new Text("C:\\Users\\jinyupeng\\Desktop\\徐家汇街道楼宇网格（专业）力量名单\\照片1\\白亚雄16602132110.jpg"),
//                "local_base64");
//        System.out.println(result);
//        byte[] data = ui.evaluate(new Text(""),
//                "local", "binary");
//        System.out.println(data.length);
    }
    public UploadImage(){
        try{
            InputStream in =  UploadImage.class.getClassLoader().getResourceAsStream("hdfs.properties");
            pps.load(in);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }
    }
    public BytesWritable evaluate(Text path, Text date_encoder_type){
        if(null == path){
            return null;
        }
        byte[] byte_data;
        String[] ss = date_encoder_type.toString().split("\\~");
        if(ss.length != 2){
            throw new IllegalArgumentException();
        }
        String date = ss[1];
        String encoder_type = ss[0];
        if(encoder_type.startsWith("local")){
            File f = new File(path.toString());
            if(f.exists()){
                if(encoder_type.endsWith("base64")){
                    byte_data = FileUtil.localImage2bytes_base64(path.toString());
                }else {
                    byte_data = FileUtil.localImage2byte(path.toString());
                }
                return new BytesWritable(byte_data);
            }
        }else if(encoder_type.startsWith("hdfs")){
            HDFSOperation hdfsOperation = new HDFSOperation();
            try {
                byte[] data;
                if(encoder_type.endsWith("base64")){
                    data = FileUtil.byteToBase64(hdfsOperation.download(new Path(path.toString()))).getBytes();
                }else {
                    data = hdfsOperation.download(new Path(path.toString()));
                }
                return new BytesWritable(data);
            }catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }else if(encoder_type.startsWith("net")){
            try {
                byte[] data;
                if (encoder_type.endsWith("base64")) {
                    data = FileUtil.byteToBase64(FileUtil.onlineImg2Bytes(path.toString())).getBytes();
                }else{
                    data = FileUtil.onlineImg2Bytes(path.toString());
                    if(data.length > 10485760){
                        String hdfs_path = uploadToHDFS(path.toString(), date, data);
                        return new BytesWritable(hdfs_path.getBytes());
//                        return null;
                    }
                }
                return new BytesWritable(data);
            }catch(Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    public String uploadToHDFS(String path, String date, byte[] binary_data) throws UnsupportedEncodingException, ParseException {
        String base_path = "/voice/";
        StringBuilder builder = new StringBuilder("http://31.0.141.194:30825/webhdfs/v1/");
        base_path += date + "/";
        String[] ss = path.split("/");
        String file_name = ss[ss.length - 1];
        base_path += file_name;
        builder.append(base_path);
        builder.append("?op=CREATE&data=TRUE&guardian_access_token=ghRCDMu8XKV5svXj6qWz-CH11409.TDH");
        HttpfsOperation operation = new HttpfsOperation();
        operation.upload(builder.toString(), binary_data);
        return base_path;
    }
}
