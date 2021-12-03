package com.hitachivantara.data.management.flight;

import com.hitachivantara.data.management.flight.read.MockProducer;
import com.hitachivantara.data.management.flight.read.MinioReadProducer;
import com.hitachivantara.data.management.flight.read.ReadProducer;
import com.hitachivantara.data.management.webservices.dto.input.Resource;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ReadConnectorFactory {

  private static final Logger log = LoggerFactory.getLogger( ReadConnectorFactory.class );

  public FlightServer getReadConnector( Location location, Resource resource ) {

    ReadProducer producer = null;

    if ( resource.getType().equals( "mock" ) ) {
      producer = new MockProducer();
    } else if ( resource.getType().equals( "minio" ) ) {
      producer = new MinioReadProducer( resource );
    }

    if ( producer != null ) {
      BufferAllocator allocator = new RootAllocator();
      FlightServer server = FlightServer.builder( allocator, location, producer ).build();
      log.info( "Running in location: " + server.getLocation().toString() );
      return server;
    }

    return null;
  }

}
