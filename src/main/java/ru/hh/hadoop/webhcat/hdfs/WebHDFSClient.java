package ru.hh.hadoop.webhcat.hdfs;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;

public class WebHDFSClient {

  private final WebTarget webHDFSResource;
  private final Client client;

  public WebHDFSClient(String baseUrl) {
    this(baseUrl, null);
  }

  public WebHDFSClient(String baseUrl, Map<String, Object> configProperties) {
    final ClientConfig clientConfig = new ClientConfig();
    if (configProperties != null) {
      clientConfig.getProperties().putAll(configProperties);
    }

    clientConfig.property(ClientProperties.FOLLOW_REDIRECTS, Boolean.FALSE);
    client = ClientBuilder.newClient(clientConfig);

    webHDFSResource = client.target(baseUrl).path("webhdfs/v1");
  }

  public void mkdir(String path) {
    webHDFSResource.path(path).queryParam("op", "MKDIRS").request().put(Entity.text(StringUtils.EMPTY));
  }

  public void sendFile(String path, boolean overwrite, String content) {
    final Response expectContinueResponse = webHDFSResource.path(path)
      .queryParam("op", "CREATE")
      .queryParam("overwrite", String.valueOf(overwrite))
      .request()
      .header("Expect", "100-Continue")
      .put(Entity.text(StringUtils.EMPTY));

    if (expectContinueResponse.getStatus() != 307) {
      throw new IllegalStateException(String.format("Unexpected response from hdfs namenode. Expected: 307, got: %d",
        expectContinueResponse.getStatus()));
    }

    final URI redirectedLocation = expectContinueResponse.getLocation();
    final Response putResponse = client.target(redirectedLocation).request().put(Entity.text(content));

    if (putResponse.getStatus() != 201) {
      throw new IllegalStateException(String.format("Unexpected response from hdfs datanode with url %s. Expected: 201, got: %d",
        redirectedLocation.toString(), putResponse.getStatus()));
    }
  }
}
