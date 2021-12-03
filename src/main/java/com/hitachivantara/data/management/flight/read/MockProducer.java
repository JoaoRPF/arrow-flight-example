package com.hitachivantara.data.management.flight.read;

import org.apache.arrow.flight.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class MockProducer extends ReadProducer {

  private static final Logger log = LoggerFactory.getLogger( MockProducer.class );
  private static final Charset charset = StandardCharsets.UTF_8;

  @Override
  public void getStream( CallContext context, Ticket ticket, ServerStreamListener listener ) {
    log.info( "Getting GET request for ticket " + new String( ticket.getBytes(), charset ) );
    listener.completed();
  }

  private String getTicketPayload( Ticket ticket ) {
    return new String( ticket.getBytes(), charset );
  }

}
