package io.transwarp.inceptor.hdfs;

import java.io.IOException;

public class HDFSOperationTest {
    public static void main(String[] args) throws IOException {
        if(args.length < 3){
            throw new IllegalArgumentException();
        }
        String command = args[0];
        String scrPath = args[1];
        String dstPath = args[2];

//        String command = "upload";
//        String scrPath = "";
//        String dstPath = "";
        HDFSOperation operation = new HDFSOperation();
        if("upload".equals(command)){
            operation.uploadLocalFileHDFS(scrPath, dstPath);
        }else if("download".equals(command)){
            operation.downloadHDFSFile(scrPath, dstPath);
        }else{
            System.out.println("unkown command!");
        }


    }
}
