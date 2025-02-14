package dev.hienph.fusionj.executor;

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

public class BallistaFlightProducer implements FlightProducer {

  @Override
  public void getStream(CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {
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
