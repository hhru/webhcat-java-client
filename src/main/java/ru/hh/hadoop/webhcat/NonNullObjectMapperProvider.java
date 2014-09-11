package ru.hh.hadoop.webhcat;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import javax.ws.rs.ext.ContextResolver;

public class NonNullObjectMapperProvider implements ContextResolver<ObjectMapper> {
  private static final ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
  }


  @Override
  public ObjectMapper getContext(Class<?> type) {
    return mapper;
  }
}
