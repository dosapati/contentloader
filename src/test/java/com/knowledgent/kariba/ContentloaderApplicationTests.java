package com.knowledgent.kariba;

import com.knowledgent.kariba.contentloader.DatabaseLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ContentloaderApplicationTests {

  @Autowired
  DatabaseLoader databaseLoader;

	@Test
	public void contextLoads() {
		System.out.println(" contextLoaded successfully....!" );

    System.out.println("databaseLoader = " + databaseLoader.loadData());


	}

}
