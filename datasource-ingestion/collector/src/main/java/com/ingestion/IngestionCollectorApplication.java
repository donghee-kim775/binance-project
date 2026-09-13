package com.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication // Springboot App 등록
@ConfigurationPropertiesScan // @ConfigurationProperties를 붙인 클래스들을 스캔해서 Bean으로 등록
public class IngestionCollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionCollectorApplication.class, args);
	}

}
