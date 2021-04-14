package service;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import akka.grpc.GrpcClientSettings;

import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;

import service.proto.MyService;



public class Main extends AbstractBehavior<Void> {

  public static void main(String[] args) throws Exception {
    ActorSystem<Void> system = ActorSystem.create(Main.create(), "MyService");
  }

  public static Behavior<Void> create() {
    return Behaviors.setup(Main::new);
  }

  public Main(ActorContext<Void> context) {
    super(context);

    ActorSystem<?> system = context.getSystem();

    AkkaManagement.get(system).start();
    ClusterBootstrap.get(system).start();

    MyServiceItem.init(system);

    
    CassandraSession session =
        CassandraSessionRegistry.get(system).sessionFor("akka.persistence.cassandra"); 

    String grpcInterface =
        system.settings().config().getString("my-service.grpc.interface");
    int grpcPort = system.settings().config().getInt("my-service.grpc.port");
    MyService grpcService = new MyServiceImpl(system);
    MyServer.start(grpcInterface, grpcPort, system, grpcService);

    
  }

  @Override
  public Receive<Void> createReceive() {
    return newReceiveBuilder().build();
  }
}
