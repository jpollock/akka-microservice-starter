package service;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.pattern.StatusReply;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 * <p>It has a state, [[MyServiceItem.State]], which holds the current shopping cart items and
 * whether it's checked out.
 *
 * <p>You interact with event sourced actors by sending commands to them, see classes implementing
 * [[MyServiceItem.Command]].
 *
 * <p>The command handler validates and translates commands to events, see classes implementing
 * [[MyServiceItem.Event]]. It's the events that are persisted by the `EventSourcedBehavior`. The
 * event handler updates the current state based on the event. This is done when the event is first
 * created, and when the entity is loaded from the database - each event will be replayed to
 * recreate the state of the entity.
 */
public final class MyServiceItem
    extends EventSourcedBehaviorWithEnforcedReplies<
        MyServiceItem.Command, MyServiceItem.Event, MyServiceItem.State> {

  interface Command {}

  interface Event {}

  public static class State {}

  public static Behavior<Command> create() {
    return new MyServiceItem(PersistenceId.ofUniqueId("pid"));
  }

  private MyServiceItem(PersistenceId persistenceId) {
    super(persistenceId);
  }

  @Override
  public State emptyState() {
    return new State();
  }

  @Override
  public CommandHandler<Command, Event, State> commandHandler() {
    return (state, command) -> {
      throw new RuntimeException("TODO: process the command & return an Effect");
    };
  }

  @Override
  public EventHandler<State, Event> eventHandler() {
    return (state, event) -> {
      throw new RuntimeException("TODO: process the event return the next state");
    };
  }
}