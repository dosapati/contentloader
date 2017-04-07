package com.knowledgent.kariba.util;

import java.io.FileReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Hari.Dosapati on 12/6/2016.
 */
public class PropertyReader {

  public static Map<String,String> getAllPropertiesFrom(String fileName){
    Map<String,String> allProps = new HashMap<>();

    Properties properties = new Properties();

    try{
      properties.load(new FileReader(fileName));
    }catch (Exception e){
      System.out.println("Error in reading a property file name named as ---> "+fileName);
      e.printStackTrace();

      return null;
    }

    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      allProps.put((String)entry.getKey(),(String)entry.getValue());
    }

    return allProps;


  }

}
