package com.atguigu.flume.interceptor;


import com.atguigu.flume.utils.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

/*
 * @ClassName: LogETLInterceptor
 * @projectName flumeInterceptor
 * @Auther: djr
 * @Date: 2019/7/18 13:32
 * @Description: 自定义拦截器
 */
@SuppressWarnings("unused")
public class LogETLInterceptor implements Interceptor {

    private static final Logger logger =  LoggerFactory.getLogger(LogETLInterceptor.class);

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        // 获取数据
        byte[] body = event.getBody();
        String message = new String(body, Charset.forName("UTF-8"));
        // 校验： 1. 启动日志 (json)
        //  2.  事件日志(服务器时间|json)
        logger.info(message);
        if(message.contains("start")){
            // 校验启动日志
            logger.info("start --> " + LogUtils.valuateStart(message));
            if(LogUtils.valuateStart(message)){
                return event;
            }
        }else{
            // 校验事件日志
            logger.info("event --> " + LogUtils.valuateStart(message));
            if(LogUtils.valuateEvent(message)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> newEventList = new ArrayList<>();
        logger.info(String.valueOf(events.size()));
        for(Event event : events){
             Event intercept = intercept(event);
             newEventList.add(intercept);
        }
        logger.info(String.valueOf(newEventList.size()));
        return newEventList;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
