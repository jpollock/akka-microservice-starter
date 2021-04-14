package service;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.grpc.GrpcServiceException;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.proto.*;

public final class MyServiceImpl implements MyService {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Duration timeout;
  private final ClusterSharding sharding;

  public MyServiceImpl(ActorSystem<?> system) {
    timeout = system.settings().config().getDuration("my-service.ask-timeout");
    sharding = ClusterSharding.get(system);
  }

  @Override
  public CompletionStage<Item> createItem(CreateItemRequest in) {
    logger.info("createItem {}", in.getName());
    EntityRef<MyServiceItem.Command> entityRef =
        sharding.entityRefFor(MyServiceItem.ENTITY_KEY, in.getName());
    CompletionStage<MyServiceItem.Summary> reply =
        entityRef.askWithStatus(
            replyTo -> new MyServiceItem.CreateItem(in.getName(), in.getDescription(), replyTo),
            timeout);
    CompletionStage<Item> item = reply.thenApply(MyServiceImpl::toProtoItem);
    return convertError(item);
  }

  @Override
  public CompletionStage<Item> updateItem(UpdateItemRequest in) {
    logger.info("readItem {}", in.getName());
    EntityRef<MyServiceItem.Command> entityRef =
        sharding.entityRefFor(MyServiceItem.ENTITY_KEY, in.getName());
    final CompletionStage<MyServiceItem.Summary> reply;
    
    reply =
        entityRef.askWithStatus(
            replyTo ->
                new MyServiceItem.UpdateItem(in.getName(), in.getDescription(), replyTo),
            timeout);
        CompletionStage<Item> item = reply.thenApply(MyServiceImpl::toProtoItem);
    return convertError(item);
  }

  
  @Override
  public CompletionStage<Item> readItem(ReadItemRequest in) {
    logger.info("readItem {}", in.readItemId());
    EntityRef<MyServiceItem.Command> entityRef =
        sharding.entityRefFor(MyServiceItem.ENTITY_KEY, in.itemId());
    CompletionStage<MyServiceItem.Summary> reply =
        entityRef.ask(replyTo -> new MyServiceItem.Get(replyTo), timeout);
    CompletionStage<Item> protoItem =
        reply.thenApply(
            item -> {
              if (item.isEmpty())
                throw new GrpcServiceException(
                    Status.NOT_FOUND.withDescription("Item " + in.readItemId() + " not found"));
              else return toProtoItem(item);
            });
    return convertError(protoItem);
  }
  
  
  private static Item toProtoItem(MyServiceItem.Summary item) {
    return MyServiceItem.newBuilder().setItemId(entry.getKey()).setQuantity(entry.getValue()).build();
  }
  

  private static <T> CompletionStage<T> convertError(CompletionStage<T> response) {
    return response.exceptionally(
        ex -> {
          if (ex instanceof TimeoutException) {
            throw new GrpcServiceException(
                Status.UNAVAILABLE.withDescription("Operation timed out"));
          } else {
            throw new GrpcServiceException(
                Status.INVALID_ARGUMENT.withDescription(ex.getMessage()));
          }
        });
  }
}
