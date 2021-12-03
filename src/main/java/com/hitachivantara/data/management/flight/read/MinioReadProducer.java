package com.hitachivantara.data.management.flight.read;

import com.hitachivantara.data.management.webservices.dto.input.Resource;
import org.apache.arrow.flight.Ticket;
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

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



    /*PutObjectRequest putRequest = PutObjectRequest.builder()
      .bucket( bucket )
      .key( "random_name" )
      .build();*/

    /* InputStream isFromFirstData = new ByteArrayInputStream(baos.toByteArray());

    try {
      PutObjectResponse putResponse = s3Client.putObject( putRequest, RequestBody.fromInputStream( isFromFirstData, isFromFirstData.available() ) );
    } catch ( Exception e ) {
      log.error( e.getMessage() );
    } */

  }

  private URI setUri() {
    try {
      return new URI( "http://localhost:9000");
    } catch ( Exception e ) {
      return null;
    }
  }

}
