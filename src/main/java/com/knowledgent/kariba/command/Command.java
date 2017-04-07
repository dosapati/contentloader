package com.knowledgent.kariba.command;

import java.util.Map;
import java.util.Set;

/**
 * Created by Hari.Dosapati on 12/6/2016.
 */
public interface Command {

  public Map<String, Set<String>> loadData(Map<String,String> configInfo);
}
