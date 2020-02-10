package com.wdl.gmall0218.logger.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @PostMapping(path = "log")
    public String doLog(@RequestParam("logString") String logString){
        System.out.println(logString);
        return "success";
    }
}
