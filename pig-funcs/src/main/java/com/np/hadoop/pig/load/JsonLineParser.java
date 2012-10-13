package com.np.hadoop.pig.load;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public interface JsonLineParser {
  /**
   * Parse a JSON Line and return a JSON Map
   * 
   * @param line Line to parse
   * @return A Map of JSON
   * @throws ParseException If an error occurs parsing
   */
  Map<String, Object> parse(String line) throws ParseException;

  /**
   * Implementation of the JSONLine Parser. This parser will not process
   * escaped JSON. For standard JSON, the value of the map is a String. If 
   * value is JSON, then the same is represented as a Map.
   */
  static class Impl implements JsonLineParser {

    @Override
    public Map<String, Object> parse(String line) throws ParseException {
      JSONParser parser = new JSONParser();
      return parse(line, parser);
    }
    
    private Map<String, Object> parse(String line, JSONParser jsonParser) throws ParseException {
      Map<String, Object> resultMap = new HashMap<String, Object>();
      JSONObject jsonObj = null;
      
      try {
        jsonObj = (JSONObject) jsonParser.parse(line);
      } catch (ClassCastException e) {
        throw new ParseException(1, "unexpected object");
      }
      
      if (jsonObj.isEmpty()) {
        return resultMap;
      }
      
      for (Object key : jsonObj.keySet()) {
        String keyStr = String.valueOf(key);
        String valueStr = String.valueOf(jsonObj.get(key));
        
        try {
          resultMap.put(keyStr, parse(valueStr));
        } catch (ParseException e) {
          // Nope not json
          resultMap.put(keyStr, valueStr);
        }
      }
      
      return resultMap;
    }
  }
}