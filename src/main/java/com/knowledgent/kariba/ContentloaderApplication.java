package com.knowledgent.kariba;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PreDestroy;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableScheduling
public class ContentloaderApplication {
  private ExecutorService executorService;

	@Bean(destroyMethod = "shutdown")
	public Executor taskScheduler() {
    this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    return this.executorService;
	}

  @PreDestroy
  public void beandestroy() {
    if(this.executorService != null){
      try {
        // wait 1 second for closing all threads
        executorService.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(ContentloaderApplication.class, args);
  }

}
