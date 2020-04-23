package com.peng.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

//@RestController=@Controller+@ResponseBody
@Slf4j
@RestController//把"success"不视为页面名称 视为普通字符串
public class LoggerController {
    @Autowired      //Spring帮着自动注入kafka
            KafkaTemplate<String, String> kafkaTemplate;

    //    @RequestMapping(name="/log",method = RequestMethod.POST)
    @PostMapping("log") //两种写法作用一样      //将请求的logString=xxx 装配到logString变量
    public String dolog(@RequestParam("logString") String logString) {
        //1,补充个时间戳
        JSONObject jo = JSON.parseObject(logString);
        jo.put("ts", System.currentTimeMillis());
        //2,写日志(用于离线采集)
        String jsonString = jo.toJSONString();
        log.info(jsonString);
        //3,发送到kafka
        if ("startup".equals(jo.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }
        return "success";
    }
}
