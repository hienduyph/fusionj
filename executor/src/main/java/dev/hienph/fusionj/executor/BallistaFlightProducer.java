package dev.hienph.fusionj.executor;

import dev.hienph.fusionj.protobuf.ProtobufDeserializer;
import java.util.Map;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class BallistaFlightProducer implements FlightProducer {

  @Override
  public void getStream(
    CallContext callContext,
    Ticket ticket,
    ServerStreamListener serverStreamListener
  ) {
    if (serverStreamListener == null) {
      throw new IllegalStateException("not found a listen stream");
    }
    if (ticket == null || ticket.getBytes() == null) {
      throw new IllegalStateException("invalid ticket stream");
    }
    try {
      process(callContext, ticket, serverStreamListener);
    } catch (Exception e) {
      serverStreamListener.error(e);
    }
  }

  private void process(
    CallContext callContext,
    Ticket ticket,
    ServerStreamListener serverStreamListener
  ) throws Exception {
    var action = dev.hienph.fusionj.protobuf.Action.parseFrom(ticket.getBytes());
    var logicalPlan = new ProtobufDeserializer().fromProto(action.getQuery());
    System.out.println(logicalPlan.pretty());

    final var schema = logicalPlan.schema();
    final var settings = Map.<String, String>of();
    final var ctx = new dev.hienph.fusionj.execution.ExecutionContext(settings);
    final var results = ctx.execute(logicalPlan);
    final var allocator = new RootAllocator(Long.MAX_VALUE);
    final var root = VectorSchemaRoot.create(schema.toArrow(), allocator);
    serverStreamListener.start(root, null);
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria,
    StreamListener<FlightInfo> streamListener) {

  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {

  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    return null;
  }


  @Override
  public void doAction(CallContext callContext, Action action,
    StreamListener<Result> streamListener) {

  }
}
