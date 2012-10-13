package com.np.hadoop.pig.func;

import java.io.IOException;
import java.util.List;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class LikeFilter extends FilterFunc {

  public Boolean exec(Tuple input) throws IOException {
    if (input == null || input.size() < 2) {
      // If no filter and input element are provided, filter provides false.
      return Boolean.FALSE;
    }
   
    List<Object> elems = input.getAll();
    
    // First element is the word presented
    Object expected = input.getAll().get(0);
    
    // Subsequent elements are the filter conditions
    List<Object> comparators = elems.subList(1, elems.size());
    
    return comparators.contains(expected);
  }
}
