package com.wdl.gmall0218.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@MapperScan(basePackages = "com.wdl.gmall0218.publisher.mapper")
public class Gmall0218PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0218PublisherApplication.class, args);
    }

}
