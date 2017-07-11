package de.hpi.akka_tutorial.remote.actors;

import akka.actor.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * The reaper resides in any actor system and terminates it as soon as all watched actors have terminated.
 */
public class Reaper extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "reaper";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link Reaper} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(Reaper.class);
	}

	/**
	 * Asks the {@link Reaper} to watch for the termination of the sender.
	 */
	public static class WatchMeMessage implements Serializable {

		private static final long serialVersionUID = -5201749681392553264L;
	}

	/**
	 * Keep track of the actors to be reaped eventually.
	 */
	private final Set<ActorRef> watchees = new HashSet<>();

	/**
	 * Haves the given {@link AbstractActor} being watched with the default reaper in the local {@link ActorSystem}.
	 *
	 * @param actor to be watched
	 * @see #DEFAULT_NAME the name of the default reaper
	 */
	public static void watchWithDefaultReaper(AbstractActor actor) {
		ActorSelection defaultReaper = actor.getContext().getSystem().actorSelection("/user/" + DEFAULT_NAME);
		defaultReaper.tell(new WatchMeMessage(), actor.getSelf());
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		log().info("Started {}...", getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(WatchMeMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().error(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	/**
	 * Process a {@link WatchMeMessage}.
	 *
	 * @param msg the message
	 */
	private void handle(WatchMeMessage msg) {
		ActorRef sender = getSender();
		if (this.watchees.add(sender)) {
			this.getContext().watch(sender);
			this.log().info("Start watching {}...", sender);
		}
	}

	/**
	 * Process a {@link Terminated} message.
	 *
	 * @param msg the message
	 */
	private void handle(Terminated msg) {
		ActorRef actor = msg.getActor();
		if (this.watchees.remove(actor)) {
			this.log().info("Reaping {}.", actor);
			if (this.watchees.isEmpty()) {
				this.log().info("Everyone has been reaped. Terminating the actor system...");
				this.getContext().getSystem().terminate();
			}
		} else {
			this.log().error("Got termination message from unwatched {}.", actor);
		}
	}

}
