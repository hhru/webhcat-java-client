package ru.hh.hadoop.webhcat.hive;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.jetty.JettyTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Test;
import ru.hh.hadoop.webhcat.WebHCatClient;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WebHCatClientTest extends JerseyTest {

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new JettyTestContainerFactory();
  }

  @Override
  protected Application configure() {
    return new ResourceConfig(TestResource.class);
  }

  @Path("templeton/v1/ddl/database")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Singleton
  public static class TestResource {

    @PUT
    @Path("{dbName}/table/{tableName}")
    public Response createTable(@PathParam("dbName") String dbName,
                                @PathParam("tableName") String tableName,
                                CreateTable createTable) {
      Map<String, Object> entity = new HashMap<>();
      entity.put("tableName", tableName);
      entity.put("databaseName", dbName);
      entity.put("reqObj", createTable);
      return Response.ok().entity(entity).build();
    }

    @DELETE
    @Path("{dbName}/table/{tableName}")
    public Response deleteTable(@PathParam("dbName") String dbName,
                                @PathParam("tableName") String tableName,
                                @QueryParam("user.name") String userName,
                                @QueryParam("ifExists") String ifExists) {
      Map<String, String> entity = new HashMap<>();
      entity.put("tableName", tableName);
      entity.put("databaseName", dbName);
      entity.put("userName", userName);
      entity.put("ifExists", ifExists);
      return Response.status(Response.Status.OK)
        .entity(entity)
        .build();
    }
  }

  @Test
  public void testDropTable() {
    final String databaseName = "testDatabase";
    final String tableName = "testTable";
    final String userName = "test.hive.user";
    final Boolean ifExists = true;

    WebHCatClient client = new WebHCatClient(getBaseUri().toString(), userName);

    Response response = client.dropTable(databaseName, tableName, ifExists);
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON_TYPE);

    Map<String, String> body = response.readEntity(Map.class);

    assertEquals(databaseName, body.get("databaseName"));
    assertEquals(tableName, body.get("tableName"));
    assertEquals(userName, body.get("userName"));
    assertEquals(ifExists, Boolean.parseBoolean(body.get("ifExists")));
  }

  @Test
  public void testCreateTable() {
    final String databaseName = "testDatabase";
    final String tableName = "testTable";
    final String tableLocation = "table_location";

    final CreateTable createTable = CreateTable.builder()
      .addColumn("vacancy_id", "INT")
      .external(tableLocation)
      .fieldsTerminatedBy("\\n")
      .storedAs("TEXTFILE")
      .build();

    WebHCatClient client = new WebHCatClient(getBaseUri().toString(), "test.hive.user");
    Response response = client.createTable(databaseName, tableName, createTable);
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON_TYPE);

    Map<String, Object> body = response.readEntity(Map.class);
    assertEquals(databaseName, body.get("databaseName"));
    assertEquals(tableName, body.get("tableName"));

    Map<String, Object> reqBody = (Map<String, Object>) body.get("reqObj");
    assertEquals(true, reqBody.get("external"));
    assertEquals(tableLocation, reqBody.get("location"));
  }
}
