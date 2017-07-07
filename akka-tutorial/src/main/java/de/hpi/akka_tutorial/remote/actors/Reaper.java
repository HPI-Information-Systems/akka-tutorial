package de.hpi.akka_tutorial.remote.actors;

import akka.actor.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * The reaper resides in any actor system and terminates it as soon as all watched actors have terminated.
 */
public class Reaper extends AbstractLoggingActor {

	/**
	 * This message tells the reaper to watch for the termination of the sender.
	 */
	public static class WatchMeMessage implements Serializable {

	}

	/**
	 * Keep track of the actors to be reaped eventually.
	 */
	private final Set<ActorRef> watchees = new HashSet<>();

	/**
	 * Create {@link Props} to create a new reaper actor.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(Reaper.class);
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		log().info("Started {}...", getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(WatchMeMessage.class, this::handleWatchMe)
				.match(Terminated.class, this::handleTerminated)
				.matchAny(object -> this.log().error(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	/**
	 * Process a {@link WatchMeMessage}.
	 *
	 * @param msg the message
	 */
	private void handleWatchMe(WatchMeMessage msg) {
		ActorRef sender = getSender();
		if (this.watchees.add(sender)) {
			getContext().watch(sender);
			log().info("Start watching {}...", sender);
		}
	}

	/**
	 * Process a {@link Terminated} message.
	 *
	 * @param msg the message
	 */
	private void handleTerminated(Terminated msg) {
		ActorRef actor = msg.getActor();
		if (this.watchees.remove(actor)) {
			log().info("Reaping {}.", actor);
			if (this.watchees.isEmpty()) {
				log().info("Everyone has been reaped. Terminating the actor system...");
				getContext().getSystem().terminate();
			}
		} else {
			log().error("Got termination message from unwatched {}.", actor);
		}
	}

}
