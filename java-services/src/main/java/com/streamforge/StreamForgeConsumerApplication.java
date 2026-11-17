package com.streamforge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class StreamForgeConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamForgeConsumerApplication.class, args);
    }
}

