package com.atguigu.flume.interceptor;


import com.atguigu.flume.utils.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/*
 * @ClassName: LogETLInterceptor
 * @projectName flumeInterceptor
 * @Auther: djr
 * @Date: 2019/7/18 13:32
 * @Description: 自定义拦截器
 */
@SuppressWarnings("unused")
public class LogETLInterceptor implements Interceptor {


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
        if(message.contains("start")){
            // 校验启动日志
            if(LogUtils.valuateStart(message)){
                return event;
            }
        }else{
            // 校验事件日志
            if(LogUtils.valuateEvent(message)){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> newEventList = new ArrayList<>();
        newEventList.forEach(event -> {
            Event intercept = intercept(event);
            if(intercept != null){
                newEventList.add(intercept);
            }
        });
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
