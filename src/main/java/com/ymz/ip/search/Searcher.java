package com.ymz.ip.search;

import com.alibaba.fastjson.JSONArray;
import com.ymz.ip.model.DataBlock;
import com.ymz.ip.model.IpSearchConstant;
import com.ymz.ip.utils.ByteUtil;
import com.ymz.ip.utils.GZipUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Searcher
 *
 * @author fc
 * @date 2020-05-25
 **/
@Slf4j
public class Searcher implements IpSearchConstant {

    private static final Object LOCK = new Object();
    private static boolean memory_mode_load = false;
    private static boolean firstSearch = true;

    /**
     * for memory mode
     */
    private static int[] ipSegments;
    private static int[] ipRegionPtr;
    private static short[] ipRegionLen;

    /**
     * for memory mode
     * the original db binary string
     */
    private static byte[] dataRegion = null;

    public static DataBlock memorySearch(long tagip) {
        int ip = (int) tagip;
        // load data
        if (!memory_mode_load) {
            synchronized (LOCK) {
                if (!memory_mode_load) {
                    loadFileToMemoryMode();
                }
            }
        }

        if (ipSegments == null || ipSegments.length == 0) {
            throw new IllegalArgumentException("initialization failed...");
        }

        int index = binarySearch(ipSegments, 0, ipSegments.length - 1, ip);

        if (index == -1) {
            return null;
        }
        // 因为ip_merge.txt中的每条记录是生成2条ip（开始和结束）的记录和地域信息构成，所以除以2取整得出该ip_merge.txt的region区域对应的地址
        int dataPtr = ipRegionPtr[index >> 1];
        short len = ipRegionLen[index >> 1];
        String region = new String(dataRegion, dataPtr, len, StandardCharsets.UTF_8).trim();

        //---------------
        // used internal memory
        if (firstSearch && devDebug) {
            long sizeOfCollection1 = RamUsageEstimator.sizeOf(ipRegionPtr);
            long sizeOfCollection2 = RamUsageEstimator.sizeOf(dataRegion);
            long sizeOfCollection3 = RamUsageEstimator.sizeOf(ipRegionPtr);
            long sizeOfCollection4 = RamUsageEstimator.sizeOf(ipRegionLen);
            String humanReadableUnits1 = RamUsageEstimator.humanReadableUnits(sizeOfCollection1);
            String humanReadableUnits2 = RamUsageEstimator.humanReadableUnits(sizeOfCollection2);
            String humanReadableUnits3 = RamUsageEstimator.humanReadableUnits(sizeOfCollection3);
            String humanReadableUnits4 = RamUsageEstimator.humanReadableUnits(sizeOfCollection4);
            System.out.println("ipRegions humanSizeOf:" + humanReadableUnits1);
            System.out.println("dataRegion humanSizeOf:" + humanReadableUnits2);
            System.out.println("ipRegions humanSizeOf:" + humanReadableUnits3);
            System.out.println("ipRegionLen humanSizeOf:" + humanReadableUnits4);
            firstSearch = false;
        }
        //--------------
        return new DataBlock(region, dataPtr);
    }

    public static DataBlock memorySearch(String ip) {
        return memorySearch(ByteUtil.ipToLong(ip));
    }

    private static int binarySearch(int[] arr, int low, int high, long searchNumber) {
        int mid;
        while (low <= high) {
            mid = (low + high) >> 1;
            if (arr[mid] > searchNumber) {
                high = mid - 1;
            } else if (arr[mid] < searchNumber) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        if (low > arr.length - 1 || high < 0) {
            return -1;
        }
        return high;
    }


    /**
     * load data into memory
     */
    private static void loadFileToMemoryMode() {
        if (GZIP) {
            GZipUtils.decompress(ByteUtil.getPath(SEARCH_DB + GZipUtils.EXT), false);
        }
        RandomAccessFile raf = null;
        try {
            long sTime = System.currentTimeMillis();
            raf = new RandomAccessFile(new File(ByteUtil.getPath(SEARCH_DB)), "r");
            byte[] headBlock = new byte[HEAD_BLOCK_LENGTH];
            raf.seek(0L);
            raf.readFully(headBlock, 0, HEAD_BLOCK_LENGTH);
            // head block
            long dataEndPtr = ByteUtil.get32Long(headBlock, 0);
            long ipSegmentsEndPrt = ByteUtil.get32Long(headBlock, 4);
            // length
            int dataLen = (int) dataEndPtr;
            // container
            dataRegion = new byte[dataLen];
            byte[] searchInfoBytes = new byte[(int) (ipSegmentsEndPrt - dataEndPtr)];
            // read file
            raf.seek(0);
            raf.readFully(dataRegion, 0, dataLen);
            raf.seek(dataEndPtr);
            raf.readFully(searchInfoBytes, 0, searchInfoBytes.length);
            // deserialization
            deserialization(searchInfoBytes);
            long eTime = System.currentTimeMillis();
            log.info("load file cost time: " + (eTime - sTime) + "ms");
            memory_mode_load = true;
        } catch (IOException o) {
            throw new RuntimeException("load file error.", o);
        } finally {
            ByteUtil.ezIOClose(raf);
            if (GZIP) {
                ByteUtil.fileDel(ByteUtil.getPath(SEARCH_DB));
            }
        }

    }

    /**
     * 用于从资源文件的字节流中载入IP文件
     */
    public static void myLoadFileToMemoryMode() {

        long sTime = System.currentTimeMillis();
        InputStream initialStream = Searcher.class.getResourceAsStream("/search.db");
        if (initialStream == null) {
            System.out.println("未读取到资源文件");
        }


        try {
            //资源转为流读取
            byte[] ipStream = toByteArray(initialStream);
            System.out.println("流式读取字节数为" + ipStream.length);

            byte[] headBlock = getByteArr(ipStream, 0, HEAD_BLOCK_LENGTH);
            long dataEndPtr = ByteUtil.get32Long(headBlock, 0);
            long ipSegmentsEndPrt = ByteUtil.get32Long(headBlock, 4);
            int dataLen = (int) dataEndPtr;
            byte[] searchInfoBytes = new byte[(int) (ipSegmentsEndPrt - dataEndPtr)];
            dataRegion = getByteArr(ipStream, 0, dataLen);

            getByteArr(ipStream, searchInfoBytes, (int)dataEndPtr, searchInfoBytes.length);
            deserialization(searchInfoBytes);

            long eTime = System.currentTimeMillis();
            System.out.println("load file cost time: " + (eTime - sTime) + "ms");
            memory_mode_load = true;
        } catch (IOException o) {
            throw new RuntimeException("load file error.", o);
        }

    }

    private static void deserialization(byte[] searchInfoBytes) {
        List<ArrayList> jsonArray = JSONArray.parseArray(new String(searchInfoBytes, StandardCharsets.UTF_8), ArrayList.class);
        ArrayList arrayList0 = jsonArray.get(0);
        ArrayList arrayList1 = jsonArray.get(1);
        ArrayList arrayList2 = jsonArray.get(2);
        ipSegments = new int[arrayList0.size()];
        ipRegionPtr = new int[arrayList1.size()];
        ipRegionLen = new short[arrayList2.size()];
        for (int i = 0; i < arrayList0.size(); i++) {
            ipSegments[i] = (int) arrayList0.get(i);
        }
        for (int i = 0; i < arrayList1.size(); i++) {
            ipRegionPtr[i] = (int) arrayList1.get(i);
            ipRegionLen[i] = (short) (int) arrayList2.get(i);
        }
    }

    private static byte[] toByteArray(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }

    /**
     * 从byte[]中抽取新的byte[]
     * @param data - 元数据
     * @param start - 开始位置
     * @param end - 结束位置
     * @return 新byte[]
     */
    public static byte[] getByteArr(byte[]data,int start ,int end){
        byte[] ret=new byte[end-start];
        for(int i=0;(start+i)<end;i++){
            ret[i]=data[start+i];
        }
        return ret;
    }

    /**
     * 从byte[]中抽取新的byte[]
     * @param data - 元数据
     * @param start - 开始位置
     * @param len - 抽取长度
     * @return 新byte[]
     */
    public static byte[] getByteArr(byte[]data, byte[] ret,  int start ,int len){
        for(int i=0; i<len; i++){
            ret[i]=data[start+i];
        }
        return ret;
    }
}
