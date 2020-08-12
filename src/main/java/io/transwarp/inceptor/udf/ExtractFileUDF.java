package io.transwarp.inceptor.udf;

import io.transwarp.inceptor.hdfs.HDFSOperation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ExtractFileUDF extends UDF {
    public static final Logger log = LogManager.getLogger(UploadImage.class);
    Properties pps = new Properties();

    public static void main(String[] args) {

        String path = "http://ig-ivr.oss-cn-shanghai.aliyuncs.com/301dd7dd8dad4db9039c7ccfadbec1b2.wav";
        ExtractFileUDF udf = new ExtractFileUDF();
        System.out.println(udf.pps.getProperty("fshdfs"));
        udf.evaluate(new Text(path), new Text("net_binary"));
    }

    public ExtractFileUDF(){
        try{
            InputStream in =  ExtractFileUDF.class.getClassLoader().getResourceAsStream("hdfs.properties");
            pps.load(in);
        }catch (IOException e){
            System.exit(1);
        }
    }

    public BytesWritable evaluate(Text path, Text mode_type){
        assert path != null && path.getLength() > 0;
        byte[] byte_data = null;
        HDFSOperation hdfsOperation = new HDFSOperation();
        String encoder_type = mode_type.toString();
        if(encoder_type.startsWith("local")){
            File f = new File(path.toString());
            if(f.exists()){
                if(encoder_type.endsWith("base64")){
                    byte_data = FileUtil.localImage2bytes_base64(path.toString());
                }else if(encoder_type.endsWith("binary")){
                    byte_data = FileUtil.localImage2byte(path.toString());
                }
            }
        }else if(encoder_type.startsWith("hdfs")){
            try {
                if(encoder_type.endsWith("base64")){
                    byte_data = FileUtil.byteToBase64(hdfsOperation.download(new Path(path.toString()))).getBytes();
                }else {
                    byte_data = hdfsOperation.download(new Path(path.toString()));
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }else if(encoder_type.startsWith("net")){
            try {
                if (encoder_type.endsWith("base64")) {
                    byte_data = FileUtil.byteToBase64(FileUtil.onlineImg2Bytes(path.toString())).getBytes();
                }else{
                    byte_data = FileUtil.onlineImg2Bytes(path.toString());
                    if(byte_data.length > 10485760){
//                        String hdfs_path = hdfsOperation.uploadFile(pps.getProperty("hdfs_path_prefix", "/tmp/"), path.toString(), byte_data);
//                        return new BytesWritable(hdfs_path.getBytes());
                    }
                }
                return new BytesWritable(byte_data);
            }catch(IOException e) {
                e.printStackTrace();
            }
        }
        return new BytesWritable(byte_data);
    }


}
