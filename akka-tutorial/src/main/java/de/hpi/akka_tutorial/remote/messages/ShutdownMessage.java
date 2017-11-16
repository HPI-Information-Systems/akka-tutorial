package de.hpi.akka_tutorial.remote.messages;

import java.io.Serializable;

/**
 * Asks an actor to complete its work and to stop its actor hierarchy afterwards.
 * The actor should, in particular, not accept any further work after this message was received.
 */
public class ShutdownMessage implements Serializable {

	private static final long serialVersionUID = 4769910842262232302L;
	
}
