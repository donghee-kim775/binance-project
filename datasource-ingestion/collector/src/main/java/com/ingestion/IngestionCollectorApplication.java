package com.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication // Springboot App 등록
@ConfigurationPropertiesScan // @ConfigurationProperties를 붙인 클래스들을 스캔해서 Bean으로 등록
public class IngestionCollectorApplication {

	// SpringApplication.run()
	// 1. yaml, ConfigMap, 환경변수 읽기
	// 2. createApplicationContext
	// 3. refreshContext => 이 때 생성자 본문 실행
	//     ㄴ ...
	//     ㄴ new CollectorRunner()
	// ...
	// 4. call Runner => CollectorRunner()
	public static void main(String[] args) {
		SpringApplication.run(IngestionCollectorApplication.class, args);
	}

}
