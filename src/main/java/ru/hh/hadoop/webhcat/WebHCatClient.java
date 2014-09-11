package ru.hh.hadoop.webhcat;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import ru.hh.hadoop.webhcat.hive.CreateTable;
import javax.ws.rs.core.MediaType;
import java.util.Map;

public class WebHCatClient {

  private final WebResource webHCatResource;

  public WebHCatClient(String baseUrl, String user) {
    this(baseUrl, user, null);
  }

  public WebHCatClient(String baseUrl, String user, Map<String, Object> configProperties){
    ClientConfig clientConfig = new DefaultClientConfig();

    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    clientConfig.getSingletons().add(new JacksonJsonProvider(mapper));

    if (configProperties != null) {
      clientConfig.getProperties().putAll(configProperties);
    }

    webHCatResource = Client.create(clientConfig).resource(baseUrl).path("templeton/v1").queryParam("user.name", user);
  }

  public void createTable(String databaseName, String tableName, CreateTable createTable){
    webHCatResource.path("ddl/database").path(databaseName).path("table").path(tableName).type(MediaType.APPLICATION_JSON).put(createTable);
  }

  public void dropTable(String databaseName, String tableName, boolean ifExists) {
    webHCatResource.path("ddl/database").path(databaseName).path("table").path(tableName).queryParam("ifExists", String.valueOf(ifExists)).delete();
  }
}
