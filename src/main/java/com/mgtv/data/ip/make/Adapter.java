package com.mgtv.data.ip.make;

import com.mgtv.data.ip.model.IpSearchConstant;
import com.mgtv.data.ip.utils.ByteUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.TreeMap;

/**
 * convert ipinfo csv file to IP_MERGE txt file
 * @author ymz
 * @date 2019/12/12 18:35
 */
@Slf4j
public class Adapter implements IpSearchConstant {
    public static void myIpInfo2IpMerge() throws Exception {
        TreeMap<Integer, String> treeMap = new TreeMap<>();
        BufferedReader bfr = new BufferedReader(new FileReader(ByteUtil.checkFileExists(ByteUtil.getPath(IP_INFO_NEW))));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(ByteUtil.checkFileAndNew(ByteUtil.getPath(IP_MERGE))));
        log.info("start IpInfo2IpMerge...");
        String str = "";
        int count = 0;
        while ((str = bfr.readLine()) != null) {
            if (str.startsWith("#")) {
                continue;
            }
            str = str.replaceAll("\"", "");
            if (str.trim().length() <= 0) {
                continue;
            }
            str = str.replaceAll("\\*", "0");
            //001.050.236.000	001.050.255.255	中国	宁夏	银川	*	电信	38.487193	106.230908	Asia/Shanghai	UTC+8	640100	86	CN	AP
            //  0               1               2       3       4       5   6       7           8            9              10
            // 使用我司的分割格式
            String[] split = str.split("\\t");
            if (split.length < 10) {
                continue;
            }
            // 更新为我司的ip格式
            String sip = split[0].trim();
            String eip = split[1].trim();
            String region = split[2].trim() + "|" + split[3].trim() + "|" + split[4].trim() + "|" + split[6].trim();
            if(devDebug) {
                if (ByteUtil.ipToInteger(eip) < ByteUtil.ipToInteger(sip)) {
                    System.out.println(sip);
                    System.out.println(eip);
                    System.out.println(region);
                    break;
                }
            }
            String line = sip + "|" + eip + "|" + region;
            treeMap.put(ByteUtil.ipToInteger(split[1]), line);
            count++;
        }
        // binary search needs to be sorted
        treeMap.forEach((k, v) -> {
            try {
                bufferedWriter.write(v);
                bufferedWriter.newLine();
            } catch (IOException e) {
                // ignore
            }
        });
        log.info("read lines: " + count);
        bufferedWriter.flush();
        bufferedWriter.close();
        log.info("build success.");
    }
}
