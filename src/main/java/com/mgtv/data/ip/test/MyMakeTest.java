package com.mgtv.data.ip.test;

import com.mgtv.data.ip.make.DatMaker;
import com.mgtv.data.ip.model.IpSearchConstant;
import com.mgtv.data.ip.utils.ByteUtil;

/**
 * 将合并后的ip文本转储为数据库文件
 * 可以通过接口中GZIP参数设置是否压缩
 */
public class MyMakeTest implements IpSearchConstant {
    public static void main(String[] args) {
        DatMaker datMaker = new DatMaker();
        datMaker.make(ByteUtil.getPath(SEARCH_DB), ByteUtil.getPath(IP_MERGE));
    }
}
