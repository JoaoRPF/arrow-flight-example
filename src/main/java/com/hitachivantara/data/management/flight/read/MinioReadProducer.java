package com.hitachivantara.data.management.flight.read;

import com.hitachivantara.data.management.webservices.dto.input.Resource;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MinioReadProducer extends ReadProducer {

  private static final Logger log = LoggerFactory.getLogger( MockProducer.class );
  private static final Charset charset = StandardCharsets.UTF_8;

  S3Client s3Client;

  private String bucket;
  private String fileName;

  public MinioReadProducer( Resource resource ) {
    String[] resourceSplit = resource.getId().split( "/" );
    this.bucket = resourceSplit[0];
    this.fileName = resourceSplit[1];
    AwsCredentialsProvider credentials = new AwsCredentialsProvider() {
      @Override
      public AwsCredentials resolveCredentials() {
        return AwsBasicCredentials.create( "minio", "minio123" );
      }
    };
    s3Client = S3Client
      .builder()
      .endpointOverride( setUri() )
      .credentialsProvider( credentials )
      .httpClient( ApacheHttpClient.create() )
      .build();
  }

  @Override
  public void getStream( CallContext context, Ticket ticket, ServerStreamListener listener ) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    GetObjectRequest getRequest = GetObjectRequest.builder()
      .bucket( bucket )
      .key( fileName )
      .build();

    s3Client.getObject( getRequest, ResponseTransformer.toOutputStream( baos ) );

    String[] csvLines = baos.toString().split("\n");

    BufferAllocator allocator = new RootAllocator( Integer.MAX_VALUE );
    // Define a parent to hold the vectors
    NonNullableStructVector parent = NonNullableStructVector.empty( "parent", allocator );

    // Create a new writer. VarCharWriterImpl would probably do as well?
    BaseWriter.ComplexWriter writer = new ComplexWriterImpl( "root", parent );

    // Initialise a writer for each column, using the header as the name
    var rootWriter = writer.rootAsStruct();
    var writers = Arrays.stream( csvLines[0].split(",") ).map( colName -> rootWriter.varChar( colName ) ).collect( Collectors.toList() );

    AtomicInteger rowIdx = new AtomicInteger(0);
    Arrays.stream( csvLines )
      .skip( 1 )
      .forEach( row -> {
        String[] rowSplit = row.split(",");
        for( int i = 0; i < rowSplit.length; i++) {
          if (rowSplit[i] != null) {
            writers.get( i ).setPosition( rowIdx.get() );
            var bytes = rowSplit[i].getBytes( StandardCharsets.UTF_8 );
            var varchar = allocator.buffer( bytes.length );
            varchar.setBytes( 0, bytes );
            writers.get( i ).writeVarChar( 0, bytes.length, varchar );
          }
        }
        rowIdx.incrementAndGet();
      } );

    var rootSchema = new VectorSchemaRoot(parent.getChild("root"));
    rootSchema.setRowCount( rowIdx.get() );

    try {
      listener.start( rootSchema );
      listener.putNext();
    } catch ( Exception e ) {
      log.error( e.getMessage() );
    }
    listener.completed();

  }
    /*try ( ArrowStreamReader reader = new ArrowStreamReader( new ByteArrayInputStream( baos.toByteArray() ), allocator ) ) {
      Schema schema = reader.getVectorSchemaRoot().getSchema();
      for( int i = 0; i < 2; i++ ) {
        VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
        reader.loadNextBatch();
        log.info( readBatch.contentToTSVString() );
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    } */



    /*PutObjectRequest putRequest = PutObjectRequest.builder()
      .bucket( bucket )
      .key( "random_name" )
      .build();*/

    /* InputStream isFromFirstData = new ByteArrayInputStream(baos.toByteArray());

    try {
      PutObjectResponse putResponse = s3Client.putObject( putRequest, RequestBody.fromInputStream( isFromFirstData, isFromFirstData.available() ) );
    } catch ( Exception e ) {
      log.error( e.getMessage() );
    }

  }*/

  private URI setUri() {
    try {
      return new URI( "http://localhost:9000");
    } catch ( Exception e ) {
      return null;
    }
  }

}
