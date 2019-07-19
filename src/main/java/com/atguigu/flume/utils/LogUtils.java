package com.atguigu.flume.utils;

import com.atguigu.flume.interceptor.LogETLInterceptor;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @ClassName: LogUtils
 * @projectName flumeInterceptor
 * @Auther: djr
 * @Date: 2019/7/18 13:58
 * @Description:
 */
public class LogUtils {
    private final static Logger logger = LoggerFactory.getLogger(LogETLInterceptor.class);

    public static boolean valuateStart(String message) {
        //json
        if(message == null){
            return false;
        }
        if(!message.trim().startsWith("{") || !message.trim().endsWith("}")){
            return false;
        }
        return true;
    }

    public static boolean valuateEvent(String message) {
        // 时间 | json
        if(message == null){
            return false;
        }
        // 1. 切割
        String[] logContents = message.split("\\|");
        // 2. 校验长度是否为2
        if(logContents.length != 2){
            return false;
        }
        if(logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }
        // 4. 判断json
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){
            return false;
        }
        return true;
    }

}
