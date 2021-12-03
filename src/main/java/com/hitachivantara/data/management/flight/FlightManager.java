package com.hitachivantara.data.management.flight;

import com.hitachivantara.data.management.flight.read.ReadProducer;
import com.hitachivantara.data.management.webservices.dto.input.Resource;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@ApplicationScoped
public class FlightManager {

  private static final Logger log = LoggerFactory.getLogger( FlightManager.class );

  @Inject
  ReadConnectorFactory readConnectorFactory;

  String host = "localhost";
  List<FlightServer> readServers = new ArrayList<>();

  public FlightServer addReadConnector( Resource resource ) {
    Location location = Location.forGrpcInsecure( "localhost", getPort() );

    FlightServer readServer = readConnectorFactory.getReadConnector( location, resource );

    try {
      readServer.start();
    } catch ( Exception e ) {
      log.error( e.getMessage() );
    }
    readServers.add( readServer );
    return readServer;
  }

  private Integer getPort() {
    return new Random().nextInt( 5999 - 5000 ) + 5000;
  }

}
