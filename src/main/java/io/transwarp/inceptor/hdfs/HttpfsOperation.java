package io.transwarp.inceptor.hdfs;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;

import java.io.*;
import java.net.URLEncoder;
import java.text.ParseException;

public class HttpfsOperation {
    public int upload(String url, byte[] binary_data) throws UnsupportedEncodingException {
        HttpClient client = new HttpClient();
        int status = -1;
        PutMethod method = new PutMethod(encoder(url));
        method.setRequestHeader("Content-Type","application/octet-stream");
        try {
            // 设置上传文件
//            File targetFile = new File(path);
//            FileInputStream in =new FileInputStream(targetFile);
            InputStream in = new ByteArrayInputStream(binary_data);
            method.setRequestBody(in);
            status = client.executeMethod(method);
        } catch (Exception e) {
            e.printStackTrace();
        }
        method.releaseConnection();
        return status;
    }


    public void listDirs(String url){
        HttpClient client = new HttpClient();
        int status = -1;
        GetMethod method = new GetMethod(url);
//        method.setRequestHeader("Content-Type","application/octet-stream");
        try {
            // 设置上传文件
            status = client.executeMethod(method);
//            System.out.println(method.getResponseBodyAsString());
            System.out.println(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        method.releaseConnection();
    }
    public void download(String url, String localPath){
        HttpClient client = new HttpClient();
        int status = -1;
        GetMethod method = new GetMethod(url);
        try {
            // 设置下载文件
            File file = new File(localPath);
            status = client.executeMethod(method);
            byte[] getData= method.getResponseBody();
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(getData);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        method.releaseConnection();
    }
    public static void main(String[] args) throws IOException, ParseException {
        String wav_path = "http://ig-ivr.oss-cn-shanghai.aliyuncs.com/record/0c00e8a9d5574f30bd2182dab0a5812b.wav";


    }
    public String encoder(String url) throws UnsupportedEncodingException {
        StringBuilder resultURL = new StringBuilder();
        for (int i = 0; i < url.length(); i++) {
            char charAt = url.charAt(i);
            //只对汉字处理
            if (isChinese(charAt) || charAt == ' ') {
                String encode = URLEncoder.encode(charAt+"","UTF-8");
                resultURL.append(encode);
            }else {
                resultURL.append(charAt);
            }
        }
        return resultURL.toString();
    }
    public boolean isChineseChar(char c) {
        Character.UnicodeScript sc = Character.UnicodeScript.of(c);
        if (sc == Character.UnicodeScript.HAN) {
            return true;
        }
        return false;
//        return String.valueOf(c).matches("[\u4e00-\u9fa5]");
    }

    private static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
    }


}
