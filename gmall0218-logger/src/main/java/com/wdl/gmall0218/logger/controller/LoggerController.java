package com.wdl.gmall0218.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wdl.gmall0218.common.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping(path = "log")
    public String doLog(@RequestParam("logString") String logString){
        //1. 加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //2. 落盘
        log.info(logString);

        //发送到kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, logString);
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, logString);
        }

        return "success";
    }
}
