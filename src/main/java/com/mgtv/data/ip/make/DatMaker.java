package com.mgtv.data.ip.make;

import com.alibaba.fastjson.JSONArray;
import com.mgtv.data.ip.model.DataBlock;
import com.mgtv.data.ip.model.IndexBlock;
import com.mgtv.data.ip.model.IpSearchConstant;
import com.mgtv.data.ip.utils.ByteUtil;
import com.mgtv.data.ip.utils.GZipUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * dbFile maker
 *
 * @author ymz
 * @date 2019-12-16 00:51
 **/
@Slf4j
public class DatMaker implements IpSearchConstant {
    private LinkedList<IndexBlock> indexPool = new LinkedList<>();
    /**
     * region and data ptr mapping data
     * key：ip_merge_txt中的区域字段的内容
     */
    private HashMap<String, DataBlock> regionPtrPool = new HashMap<>();

    /**
     * make the Db file
     *
     * @param tagDbFilePath
     * @param srcFilePath
     */
    public void make(String tagDbFilePath, String srcFilePath) {
        log.info("|--tagDbFilePath: " + tagDbFilePath);
        log.info("|--srcFilePath: " + srcFilePath);
        System.out.println();
        BufferedReader ipReader = null;
        RandomAccessFile raf = null;
        try {
            // bdFile
            File tagDbFile = ByteUtil.checkFileAndNew(tagDbFilePath);
            File srcIpFile = new File(srcFilePath);
            log.info("+-Try to load the file ...");
            log.info("|--[Ok]");
            ipReader = new BufferedReader(new FileReader(srcIpFile));

            // 初始化数据库文件

            raf = new RandomAccessFile(tagDbFile, "rw");
            //init the db file
            raf.seek(0L);
            //store the serialized object file pointer
            raf.write(new byte[HEAD_BLOCK_LENGTH]);
            log.info("+-Db file initialized.");
            log.info("+-Try to write the data blocks ... ");

            // 至此的数据库文件结构为
            /*
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            |                     Header Index(8 bytes empty)              |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            */
            String line = null;
            int count = 0;
            while ((line = ipReader.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                if (line.charAt(0) == '#') {
                    continue;
                }
                //1. get the start ip
                int sIdx = 0, eIdx = 0;
                eIdx = line.indexOf('|', sIdx + 1);
                if (eIdx == -1) {
                    continue;
                }
                String startIp = line.substring(sIdx, eIdx);
                //2. get the end ip
                sIdx = eIdx + 1;
                eIdx = line.indexOf('|', sIdx + 1);
                if (eIdx == -1) {
                    continue;
                }
                String endIp = line.substring(sIdx, eIdx);
                //3. get the region
                sIdx = eIdx + 1;
                String region = line.substring(sIdx);

                // 将起始结束IP(转为了int)以及区域信息，构建一个indexblock,然后添加到indexPool链表中
                addDataBlock(raf, startIp, endIp, region);
                count++;
            }


            log.info("|--Data block flushed!");
            log.info("|--Reader lines: " + count);
            log.info("|--Data file pointer: " + raf.getFilePointer() + "\n");
            // 至此的数据库文件结构为
            /*
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            |                     Header Index(8 bytes empty)              |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            | region1, region2, region3, ... region_N( N*datLen btye)      |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
             */

            // ipSegments 每个indexBlock的开始、结束的IP值
            int[] ipSegments = new int[indexPool.size() * 2];

            // ipRegionPtr 每个indexBlock的region文件指针地址
            int[] ipRegionPtr = new int[indexPool.size()];

            // ipRegionLen 每个indexBlock的的region数据的字节长度
            short[] ipRegionLen = new short[indexPool.size()];

            int index = 0;
            int ipRegionPtrIndex = 0;
            int ipRegionLenIndex = 0;
            for (IndexBlock block : indexPool) {
                ipSegments[index++] = block.getStartIp();
                ipSegments[index++] = block.getEndIp();
                ipRegionPtr[ipRegionPtrIndex++] = (int) block.getDataPtr();
                ipRegionLen[ipRegionLenIndex++] = block.getDataLen();
            }

            // 将以上三个数组按照开始结束ip、region数据指针、region数据长度顺序构建一个list
            // 并转存为字节数组 searchInfoBytes
            // serialize ipsegments
            List<Object> list = new ArrayList<>(3);
            list.add(ipSegments);
            list.add(ipRegionPtr);
            list.add(ipRegionLen);
            byte[] searchInfoBytes = JSONArray.toJSONBytes(list);

            // data end prt
            //SearchInfo区 起始位置的文件指针（Data区末尾的文件指针紧接searchInfo区）
            long dataEndPrt = raf.getFilePointer();
            log.info("+--Try to write searchInfo block ... ");
            raf.write(searchInfoBytes);
            long ipSegmentsEndPrt = raf.getFilePointer(); //SearchInfo区 结束位置的文件指针
            log.info("|--[Ok]");
            // 至此的数据库文件结构为
            /*
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            |                     Header Index(8 bytes empty)              |  <-- Header Index
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            | region1, region2, region3, ... region_N( N*datLen btyes)     |  <-- Data
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            | ipSegments[1..2n], ipRegionPtr[1..n], ipRegionLen[1..n]      |  <-- SearchInfo
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
             */

            // head block
            // 将Data区的结束位置文件指针和SearchInfo区的结束位置文件指针写回
            // 数据库前8个字节
            log.info("+-Try to write head block ... ");
            byte[] headBlockBytes = new byte[HEAD_BLOCK_LENGTH];
            // Data区的结束位置文件指针转储为字节（4个）
            ByteUtil.write32Long(headBlockBytes, 0, dataEndPrt);
            // SearchInfo区的结束位置文件指针转储为字节（4个）
            ByteUtil.write32Long(headBlockBytes, 4, ipSegmentsEndPrt);
            raf.seek(0L);
            raf.write(headBlockBytes);
            raf.close();
            raf = null;
            log.info("|--[Ok]");

            // 至此的数据库文件结构为
            /*
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            | Header Index(4bytes to DataEndPtr, 4bytes to SearchInfoPtr)  |  <-- Header Index
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            | region1, region2, region3, ... region_N( N*datLen bytes)     |  <-- Data
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            | ipSegments[1..2n], ipRegionPtr[1..n], ipRegionLen[1..n]      |  <-- SearchInfo
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
            */

            if (GZIP) {
                log.info("+--Try to gzip ... ");
                GZipUtils.compress(tagDbFilePath,true);
                log.info("|--[Ok]");
            }

            //print the copyright and the release timestamp info
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z");
            String copyright = "Created by fc at " + dateFormat.format(cal.getTime());
            log.info("|--[copyright] " + copyright);
            log.info("|--[Ok]");
        } catch (Exception e) {
            log.error("make error.", e);
        } finally {
            ByteUtil.ezIOClose(ipReader);
            ByteUtil.ezIOClose(raf);
        }
    }

    /**
     * internal method to add a new data block record
     *
     * @param raf
     * @param startIp
     * @param endIp
     * @param region  data
     */
    private void addDataBlock(RandomAccessFile raf, String startIp, String endIp, String region) {
        try {
            byte[] data = region.getBytes(StandardCharsets.UTF_8);
            long dataPtr = 0;
            //check region ptr pool first
            // 检查是否有重复的区域
            if (regionPtrPool.containsKey(region)) {
                DataBlock dataBlock = regionPtrPool.get(region);
                dataPtr = dataBlock.getDataPtr();
            } else {
                //This method returns the offset from the beginning of the file, in bytes
                //将新的区域写入在Header Index Block之后, 并将区域的文件指针存放于dataPtr之中
                dataPtr = raf.getFilePointer();
                raf.write(data);
                regionPtrPool.put(region, new DataBlock(region, dataPtr));
            }
            //add the data index blocks
            //将当前记录中起始、结束IP、区域信息的文件指针地址、区域信息的信息长度存放于一个
            //新构建的indexBlock中；
            IndexBlock ib = new IndexBlock(ByteUtil.ipToInteger(startIp),
                    ByteUtil.ipToInteger(endIp), dataPtr, (short) data.length);
            //将新构建的indexBlock加入indexPool链表
            indexPool.add(ib);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
