package com.peng.gmall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//自动扫描mapper 根据接口和xml创建实现类 还需要在application.properties里配xml的位置
@SpringBootApplication
@MapperScan(basePackages = "com.peng.gmall.publisher.mapper")
public class GmallPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisherApplication.class, args);
    }

}
