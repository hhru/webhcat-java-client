package ru.hh.hadoop.webhcat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import ru.hh.hadoop.webhcat.hive.CreateTable;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

public class WebHCatClient {

  private final WebTarget webHCatTarget;

  public WebHCatClient(String baseUrl, String user) {
    this(baseUrl, user, null);
  }

  public WebHCatClient(String baseUrl, String user, Map<String, Object> configProperties) {
    ClientConfig clientConfig = new ClientConfig();

    if (configProperties != null) {
      clientConfig.getProperties().putAll(configProperties);
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    clientConfig.register(mapper);
    Client client = ClientBuilder.newClient(clientConfig);
    client.register(JacksonFeature.class);

    webHCatTarget = client.target(baseUrl).path("templeton/v1").queryParam("user.name", user);
  }

  public Response createTable(String databaseName, String tableName, CreateTable createTable) {
    return webHCatTarget.path("ddl/database").path(databaseName).path("table").path(tableName)
      .request().put(Entity.entity(createTable, MediaType.APPLICATION_JSON));
  }

  public Response dropTable(String databaseName, String tableName, boolean ifExists) {
    return webHCatTarget.path("ddl/database").path(databaseName).path("table").path(tableName)
      .queryParam("ifExists", String.valueOf(ifExists))
      .request().delete();
  }
}
