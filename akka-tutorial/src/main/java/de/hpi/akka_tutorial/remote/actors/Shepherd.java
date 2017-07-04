package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.akka_tutorial.remote.Calculator;

public class Shepherd extends AbstractLoggingActor {

	public static Props props(final Calculator calculator) {
		return Props.create(Shepherd.class, () -> new Shepherd(calculator));
	}
	
	public static class Subscription implements Serializable {
		
		private static final long serialVersionUID = 6122957437037004535L;

		private final String name;
		private final String ip;
		private final String port;

		public String getName() {
			return this.name;
		}

		public String getIp() {
			return this.ip;
		}

		public String getPort() {
			return this.port;
		}

		public Subscription(final String name, final String ip, final String port) {
			this.name = name;
			this.ip = ip;
			this.port = port;
		}
		
		@Override
		public String toString() {
			return "akka.tcp://" + this.getName() + "@" + this.getIp() + ":" + this.getPort();
		}
	}
	
	private final Calculator calculator;
	
	public Shepherd(final Calculator calculator) {
		this.calculator = calculator;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Subscription.class, s -> {
					this.calculator.addSubscription(s); 
					this.log().info("New subscription: " + s.toString());})
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
}
