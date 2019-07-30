package com.sjj;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.Banner;
import com.sjj.service.impl.OperateHiveWithSpark;

import lombok.extern.slf4j.Slf4j;

/**
 * springboot启动类
 */
@Slf4j
@SpringBootApplication
public class SprakStreamingMain implements CommandLineRunner {

	@Autowired
	private OperateHiveWithSpark wordCount;

	public static void main(String[] args) throws Exception {

		// disabled banner, don't want to see the spring logo
		SpringApplication app = new SpringApplication(SprakStreamingMain.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.run(args);
	}

	// Put your logic here.
	@Override
	public void run(String[] args) throws Exception {
		try {
			wordCount.launch();
		} catch (Exception e) {
			log.error("### exception exit ###");
			log.error(e.getMessage());
			System.exit(1);
		}
	}
}
