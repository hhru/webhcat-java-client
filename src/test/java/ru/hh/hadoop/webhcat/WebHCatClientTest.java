package ru.hh.hadoop.webhcat;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import ru.hh.hadoop.webhcat.hive.Column;
import ru.hh.hadoop.webhcat.hive.CreateTable;
import ru.hh.hadoop.webhcat.hive.TableFormat;
import ru.hh.jersey.test.ActualRequest;
import ru.hh.jersey.test.ExpectedResponse;
import ru.hh.jersey.test.HttpMethod;
import ru.hh.jersey.test.JerseyClientTest;
import ru.hh.jersey.test.RequestMapping;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WebHCatClientTest extends JerseyClientTest {

  private final String hiveUser = "test.hive.user";

  @Test
  public void testDropTable() throws Exception {
    final String databaseName = "testDatabase";
    final String tableName = "testTable";

    final WebHCatClient webHCatClient = new WebHCatClient(getBaseURI().toString(), hiveUser);

    final RequestMapping requestMapping =
      RequestMapping.builder(HttpMethod.DELETE, "/templeton/v1/ddl/database/" + databaseName + "/table/" + tableName)
        .addQueryParam("user.name", hiveUser)
        .addQueryParam("ifExists", "true")
        .build();

    final ExpectedResponse expectedResponse = ExpectedResponse.builder()
      .content("{ \"table\": \"" + tableName + "\", \"database\": \"" + databaseName + "\" }")
      .mediaType("application/json")
      .build();
    setServerAnswer(requestMapping, expectedResponse);

    webHCatClient.dropTable(databaseName, tableName, true);
  }

  @Test
  public void testCreateTable() throws Exception {
    final String databaseName = "testDatabase";
    final String tableName = "testTable";

    final WebHCatClient webHCatClient = new WebHCatClient(getBaseURI().toString(), hiveUser);

    final RequestMapping requestMapping = RequestMapping.builder(HttpMethod.PUT, "/templeton/v1/ddl/database/" + databaseName + "/table/" + tableName)
      .addQueryParam("user.name", hiveUser)
      .build();
    final ExpectedResponse expectedResponse = ExpectedResponse.builder()
      .content("{ \"table\": \"" + tableName + "\", \"database\": \"" + databaseName + "\" }")
      .mediaType("application/json")
      .build();
    setServerAnswer(requestMapping, expectedResponse);

    final CreateTable createTable = CreateTable.builder()
      .addColumn("vacancy_id", "INT")
      .external("table_location")
      .fieldsTerminatedBy("\\n")
      .storedAs("TEXTFILE")
      .build();

    webHCatClient.createTable(databaseName, tableName, createTable);

    final List<ActualRequest> actualRequests = getActualRequests(requestMapping);
    assertEquals(1, actualRequests.size());

    final ActualRequest actualRequest = actualRequests.get(0);
    ObjectMapper objectMapper = new ObjectMapper();
    final CreateTable actualCreateTable = objectMapper.readValue(actualRequest.getContent(), CreateTable.class);

    assertTrue(actualCreateTable.getExternal());
    assertEquals("table_location", actualCreateTable.getLocation());
    final TableFormat actualTableFormat = actualCreateTable.getFormat();
    assertEquals("\\n", actualTableFormat.getRowFormat().getFieldsTerminatedBy());
    assertEquals("TEXTFILE", actualTableFormat.getStoredAs());

    final List<Column> actualColumns = actualCreateTable.getColumns();
    assertEquals(1, actualColumns.size());
    assertEquals("vacancy_id", actualColumns.get(0).getName());
    assertEquals("INT", actualColumns.get(0).getType());
  }
}
