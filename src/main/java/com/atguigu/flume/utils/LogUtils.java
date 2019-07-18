package com.atguigu.flume.utils;

import org.apache.commons.lang.math.NumberUtils;

/*
 * @ClassName: LogUtils
 * @projectName flumeInterceptor
 * @Auther: djr
 * @Date: 2019/7/18 13:58
 * @Description:
 */
public class LogUtils {

    public static boolean valuateStart(String message) {
        //json
        return message != null && message.trim().startsWith("{") && message.trim().endsWith("}");
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
        if(logContents[0].length() != 13 || NumberUtils.isDigits(logContents[0])){
            return false;
        }
        // 4. 判断json
        return message.trim().startsWith("{") && message.trim().endsWith("}");
    }

}
