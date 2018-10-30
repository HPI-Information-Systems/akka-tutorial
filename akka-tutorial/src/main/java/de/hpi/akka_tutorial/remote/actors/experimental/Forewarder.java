package de.hpi.akka_tutorial.remote.actors.experimental;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Forewarder extends AbstractLoggingActor {

	private ActorRef child = null;
	private int n;
	
	public Forewarder() {
		this.node = receiveBuilder()
				.match(ActorRef.class, ref -> this.child = ref)
				.match(String.class, s -> {
					this.n = Integer.valueOf(s);
					this.getContext().become(this.end);
				})
				.match(Long.class, l -> this.child.tell(l + 1, this.self()))
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
		
		this.end = receiveBuilder()
				.match(Long.class, l -> this.log().info(String.valueOf(System.currentTimeMillis() - l - this.n)))
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private Receive node;
	
	private Receive end;
	
	@Override
	public Receive createReceive() {
		return node;
	}

	/* Testing code that uses the Forewarder to measure the communication costs of Akka actors. */
	public static void test(ActorSystem actorSystem) {
		int hops = 100000;
		
		// Setup chain of actors
		ActorRef first = actorSystem.actorOf(Props.create(Forewarder.class));
		ActorRef a = first;
		for (int i = 1; i < hops; i++) {
			ActorRef b = actorSystem.actorOf(Props.create(Forewarder.class));
			a.tell(b, ActorRef.noSender());
			a = b;
		}
		a.tell(String.valueOf(hops), ActorRef.noSender());
		
		// Wait for setup to be done
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// Send message through
		first.tell(System.currentTimeMillis(), ActorRef.noSender());
		
		// Measure the simple loop time for comparison
		Long t = System.currentTimeMillis();
		for (int i = 0; i < hops; i ++)
			t = t + 1;
		System.out.println(System.currentTimeMillis() - t - hops);
	}	
}
