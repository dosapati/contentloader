package com.knowledgent.kariba.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Created by Hari.Dosapati on 12/6/2016.
 */
@Component
public class SolrLoader {
  private static final Logger log = LoggerFactory.getLogger(SolrLoader.class);


  //String urlString = "http://localhost:8983/solr";


  @PostConstruct
  public void init() {

  }

  @Value("${solr.url}")
  private String solrURL;


  public boolean update(String collection,List<Map<String, Set<String>>> data) {


    return false;
  }

  public boolean updateData(String collection,String solrColName, Map<String, Set<String>> data) {
    SolrClient solr = null;
    solr = new HttpSolrClient.Builder(solrURL+"/"+collection).build();
    boolean idExist = false;

    try {
      List<SolrInputDocument> inputDocuments = new LinkedList<>();

      for (Map.Entry<String, Set<String>> entry : data.entrySet()) {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id",entry.getKey().trim(),1.0f);
        document.addField(solrColName, StringUtils.join(entry.getValue().toArray(), " "),1.0f);
        log.info("Adding solr input doc to the list as --> {}",document.get("id"));
        inputDocuments.add(document);
      }

      log.info("Sending {} docs to be inserted into solr ",inputDocuments.size());
      UpdateResponse response = solr.add(inputDocuments);
      // Remember to commit your changes!
      solr.commit();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    return true;

  }



  public boolean updateData(String collection, List<Map<String, Set<String>>> data) {
    SolrClient solr = null;
    solr = new HttpSolrClient.Builder(solrURL+"/"+collection).build();
    boolean idExist = false;

    try {
      List<SolrInputDocument> inputDocuments = new LinkedList<>();
      for (Map<String, Set<String>> map : data) {
        if(map.get("id") != null){
          idExist = false;
        }
        SolrInputDocument document = new SolrInputDocument();

        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
          //document.addField(entry.getKey()+"_txt", entry.getValue());
        }
        if(!idExist){
          document.addField("id",UUID.randomUUID().toString(),1.0f);
          //document.addField("title", "Testing", 1.0f);
          //document.addField("description", "Testing", 1.0f);
        }
        log.info("Adding solr input doc to the list as --> {}",document.get("id"));
        inputDocuments.add(document);
      }
      log.info("Sending {} docs to be inserted into solr ",inputDocuments.size());
      UpdateResponse response = solr.add(inputDocuments);
      // Remember to commit your changes!
      solr.commit();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    return true;

  }


}
