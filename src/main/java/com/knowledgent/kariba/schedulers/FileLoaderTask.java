package com.knowledgent.kariba.schedulers;

import com.knowledgent.kariba.command.Command;
import com.knowledgent.kariba.solr.SolrLoader;
import com.knowledgent.kariba.util.Constants;
import com.knowledgent.kariba.util.PropertyReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Hari.Dosapati on 11/28/2016.
 */
@Component
public class FileLoaderTask implements ApplicationContextAware {

  @Value("${dataloader.props.files.location}")
  private String dataloaderPropsFilesLocation;

  @Autowired
  SolrLoader solrLoader;

  private static final Logger log = LoggerFactory.getLogger(FileLoaderTask.class);

  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

  @Scheduled(fixedRate = 5000)
  public void reportCurrentTime() throws Exception {
    try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(
        Paths.get(dataloaderPropsFilesLocation), "*_loader.properties")) {

      for (Path path : dirStream) {
        processEachPropsFile(path);
      }

    }
    log.info("The time is now {} , location -> {}", dateFormat.format(new Date()),dataloaderPropsFilesLocation);
  }

  private void processEachPropsFile(Path filePath) {

    log.info("Processing {} @ location -> {}", filePath.getFileName(),dataloaderPropsFilesLocation);
    //TODO:
    /**
     * 1. First move it to a temp location, so that it won't be picked out again ..
     * 2.
     */
    String runFolderPath = System.getProperty("user.dir")+"/temp/run";
    try {
      Files.createDirectories(Paths.get(runFolderPath));
    } catch (IOException e) {
      log.error("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n UNABLE to create required directories --->>>"+runFolderPath+" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");
      System.exit(-1);
    }

    try {
      String targetFileName = new StringBuilder().append(runFolderPath).append("/").append(filePath.getFileName().toString().replace("_loader","_loader_inprocess")).append("_701_").append(UUID.randomUUID().toString()).append(".properties").toString();
      Files.copy(filePath,Paths.get(targetFileName), StandardCopyOption.COPY_ATTRIBUTES);
      //delete the existing file from the loader fodler ... so tht it won't be picked out again..
      Files.delete(filePath);

      //now read the property file...
      Map<String,String> allProps  = PropertyReader.getAllPropertiesFrom(targetFileName);

      if(allProps.get(Constants.COMMAND_KEY) == null){

        System.err.println("No Command is given in the current properties file ---> "+targetFileName+". No processing will happen now.");
        log.error("No Command is given in the current properties file --->"+targetFileName+". No processing will happen now.");

      }else{

        Command t1 = (Command) context.getBean(allProps.get(Constants.COMMAND_KEY)+"Command");
        log.info("Found the command class as -->"+t1.getClass().getName());
        Map<String, Set<String>> retData = t1.loadData(allProps);

        //if some thing ecist call the solr loader
        if (retData.size() > 0) {
          log.info("sending datamap of size {} to insert into solr collection named as --> {}",retData.size(),allProps.get("table.name"));

          solrLoader.updateData(allProps.get("solr.collection.name"),allProps.get("solr.colname"), retData);

          log.info("Done inserting datamap of size {} into solr collection named as --> {}",retData.size(),allProps.get("table.name"));

        }


      }

      System.out.println("targetFileName = " + targetFileName);
    } catch (IOException e) {
      e.printStackTrace();
    }



  }


  private ApplicationContext context;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

    this.context = applicationContext;
  }
}
