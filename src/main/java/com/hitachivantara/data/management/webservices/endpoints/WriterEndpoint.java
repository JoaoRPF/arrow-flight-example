package com.hitachivantara.data.management.webservices.endpoints;

import com.hitachivantara.data.management.webservices.dto.input.Resource;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path( "writers" )
public class WriterEndpoint {

  @POST
  @Consumes( MediaType.APPLICATION_JSON )
  @Produces( MediaType.TEXT_PLAIN )
  public String create( Resource resourceDto ) {
    return "writer: " + resourceDto.getId();
  }
}
