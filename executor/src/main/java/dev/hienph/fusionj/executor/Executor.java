package  dev.hienph.fusionj.executor;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {
  private static final Logger log = LoggerFactory.getLogger(Executor.class);

  public static void main(String[] args) throws Exception {
    var name = Executor.class.getPackage().getImplementationTitle();
    var version = Executor.class.getPackage().getImplementationVersion();

    System.setProperty("io.netty.tryReflectionSetAccessible", "true");
     final var bindHost = "0.0.0.0";
     final var port = 50051;

     final var server = FlightServer.builder(
       new RootAllocator(Long.MAX_VALUE),
       Location.forGrpcInsecure(bindHost, port),
       new BallistaFlightProducer()
     ).build();
     server.start();
     log.info("Listening on %s:%s", bindHost, port);
     while (true) {
       Thread.sleep(1000);
     }
  }
}