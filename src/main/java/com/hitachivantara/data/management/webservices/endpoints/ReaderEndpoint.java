package com.hitachivantara.data.management.webservices.endpoints;

import com.hitachivantara.data.management.flight.FlightManager;
import com.hitachivantara.data.management.webservices.dto.input.Resource;
import org.apache.arrow.flight.FlightServer;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path( "readers" )
public class ReaderEndpoint {

  @Inject
  FlightManager flightManager;

  @GET
  @Produces( MediaType.APPLICATION_JSON )
  public List<String> get() {
    return new ArrayList<>();
  }


  @POST
  @Consumes( MediaType.APPLICATION_JSON )
  @Produces( MediaType.TEXT_PLAIN )
  public String create( Resource resourceDto ) {
    FlightServer readServer = flightManager.addReadConnector( resourceDto );
    return readServer.getLocation().getUri().toString();
  }
}
