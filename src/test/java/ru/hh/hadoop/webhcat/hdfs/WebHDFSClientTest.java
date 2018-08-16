package ru.hh.hadoop.webhcat.hdfs;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.jetty.JettyTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Test;

import javax.inject.Singleton;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.net.URI;

public class WebHDFSClientTest extends JerseyTest {

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new JettyTestContainerFactory();
  }

  @Override
  protected Application configure() {
    return new ResourceConfig(TestResource.class);
  }

  @Path("")
  @Singleton
  public static class TestResource {
    @PUT
    @Path("webhdfs/v1/{path}")
    public Response doSmth(@HeaderParam("Expect") String expect,
                           @PathParam("path") String path,
                           @QueryParam("op") String operation,
                           @QueryParam("overwrite") Boolean overwrite,
                           String data) {
      if (("100-Continue".equals(expect) && "CREATE".equals(operation)) || "MKDIRS".equals(operation)) {
        switch (path) {
          case "noRedirectStatus":
            return Response.ok().build();
          case "noCreatedStatus":
            return Response.status(Response.Status.TEMPORARY_REDIRECT)
              .location(URI.create("sendDataPath/noCreatedStatus"))
              .build();
          default:
            return Response.status(Response.Status.TEMPORARY_REDIRECT)
              .location(URI.create("sendDataPath/" + path))
              .build();
        }
      } else {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
    }

    @PUT
    @Path("sendDataPath/{forTest}")
    public Response sendData(@PathParam("forTest") String forTest) {
      if ("noCreatedStatus".equals(forTest)) {
        return Response.ok().build();
      } else {
        return Response.status(Response.Status.CREATED).build();
      }
    }
  }

  @Test
  public void testMkDir() {
    final String dirPath = "/testDir";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseUri().toString());
    webHDFSClient.mkdir(dirPath);
  }

  @Test
  public void testSendFile() {
    final String filePath = "/testFile";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseUri().toString());
    webHDFSClient.sendFile(filePath, true, "test content");
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExceptionWhenNoRedirectStatusInAnswerToExpectContinueSendFile() {
    final String filePath = "/noRedirectStatus";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseUri().toString());
    webHDFSClient.sendFile(filePath, true, "test content");
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExceptionWhenNoCreatedStatusInAnswerToSendFile() {
    final String filePath = "/noCreatedStatus";
    final WebHDFSClient webHDFSClient = new WebHDFSClient(getBaseUri().toString());
    webHDFSClient.sendFile(filePath, true, "test content");
  }
}
