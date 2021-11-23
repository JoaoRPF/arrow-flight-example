package com.example;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class Node2 {

  private static final Logger LOGGER  = Logger.getLogger( "Node2" );

  public static void main( String[] args ) {
    try {
      startFlightServer();
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  private static void startFlightServer() throws Exception {
    LOGGER.info("Listening in port 8816...");
    BufferAllocator allocator = new RootAllocator( Integer.MAX_VALUE );
    FlightServer.builder()
      .allocator( allocator )
      .location( Location.forGrpcInsecure( "0.0.0.0", 8816 ) )
      .producer( new FlightProducer() {
        @Override
        public void getStream( CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener ) {
          LOGGER.info("getting data...");
        }

        @Override
        public void listFlights( CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener ) {
        }

        @Override
        public FlightInfo getFlightInfo( CallContext callContext, FlightDescriptor flightDescriptor ) {
          return null;
        }

        @Override
        public Runnable acceptPut( CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener ) {
          try {
            LOGGER.info("Received a new message...");
            VarCharVector vecToAdd = new VarCharVector( "powerCategory", new RootAllocator( Integer.MAX_VALUE ) );
            vecToAdd.allocateNew();
            int countRows = 0;
            FlightClient.ClientStreamListener stream = null;

            try ( final BufferAllocator allocator = new RootAllocator( Integer.MAX_VALUE );
                  final FlightClient client = FlightClient.builder( allocator, Location.forGrpcInsecure( "0.0.0.0", 8817 ) ).build() ) {
              while ( flightStream.next() ) {
                countRows = 0;
                VectorSchemaRoot root = flightStream.getRoot();

                LOGGER.info( "\n" + root.contentToTSVString() );
                BigIntVector powerVector = (BigIntVector) root.getVector( "power" );
                for ( int i = 0; i < root.getRowCount(); i++ ) {
                  vecToAdd.setSafe( i + countRows, getCategoryFromPower( powerVector.get( i ) ) );
                }
                countRows += root.getRowCount();

                vecToAdd.setValueCount( countRows );
                VectorSchemaRoot root2 = root.addVector( 2, vecToAdd );

                LOGGER.info( "\n" + root2.contentToTSVString() );

                stream = client.startPut( FlightDescriptor.path( "superheroes" ), root2, new AsyncPutListener() );
                stream.putNext();
                stream.completed();
                stream.getResult();
              }

            }

          } catch ( Exception e ) {
            e.printStackTrace();
          }

          return () -> LOGGER.info("DoPut has ended");
        }

        @Override
        public void doAction( CallContext callContext, Action action, StreamListener<Result> streamListener ) {

        }

        @Override
        public void listActions( CallContext callContext, StreamListener<ActionType> streamListener ) {

        }
      } )
      .build()
      .start()
      .awaitTermination();
  }

  private static byte[] getCategoryFromPower( long power ) {
    String s;
    if ( power <= 3 ) {
      s = "WEAK";
    }
    if ( power >= 4 && power <= 5 ) {
      s = "MID";
    }
    if ( power >= 6 && power <= 7 ) {
      s = "MID+";
    }
    if ( power < 10 ) {
      s = "STRONG";
    } else {
      s = "TOP";
    }
    return s.getBytes( StandardCharsets.UTF_8 );
  }
}
