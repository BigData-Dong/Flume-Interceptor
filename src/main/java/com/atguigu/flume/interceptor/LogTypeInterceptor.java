package com.atguigu.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * @ClassName: LogTypeInterceptor
 * @projectName flumeInterceptor
 * @Auther: djr
 * @Date: 2019/7/18 14:16
 * @Description:
 */
@SuppressWarnings("unused")
public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        // json - start event 放到header
        // 1. 获取body数据
        byte[] body = event.getBody();
        String message = new String(body,Charset.forName("UTF-8"));
        //2. 获取header
        Map<String, String> headers = event.getHeaders();
        //3. 向header中添加值
        if(message.contains("start")){
            headers.put("topic","topic_start");
        }else {
            headers.put("topic","topic_event");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> list = new ArrayList<>();
        events.forEach(item -> {
            Event event = intercept(item);
            list.add(event);
        });
        return list;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
