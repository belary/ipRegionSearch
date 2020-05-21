package com.ymz.ip.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class MyTest {

    public static void main(String[] args) {
        try {

            // return the file pointer
            RandomAccessFile raf = new RandomAccessFile("d:/test.txt", "rw");
            String tmpStr = "Hello";
            byte[] datArr = tmpStr.getBytes(StandardCharsets.UTF_8);
            raf.write(datArr);
            System.out.println("" + raf.getFilePointer());

            // 证明每次write以后文件指针会自动移动到下一个字节
            byte[] datArr2 = "belary".getBytes(StandardCharsets.UTF_8);
            raf.write(datArr2);
            System.out.println("" + raf.getFilePointer());

            raf.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
