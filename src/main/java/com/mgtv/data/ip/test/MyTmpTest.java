package com.mgtv.data.ip.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class MyTmpTest {

    public static void main(String[] args) throws InterruptedException {

//        long tmpl = 1594264890000L;
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        String tmp = sdf.format(tmpl );
//
//        System.out.println(tmp);


        Timestamp d1 = new Timestamp(System.currentTimeMillis());

        Thread.sleep(5000);

        Timestamp d2 = new Timestamp(System.currentTimeMillis());
        long millis = d2.getTime() - d1.getTime();
        long seconds = millis ;

        System.out.println( " 耗时秒数:" + seconds);

    }
}
