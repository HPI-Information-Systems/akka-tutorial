package de.hpi.akka_tutorial.remote.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.Arrays;

/**
 * This class contains tests for {@link Worker}s.
 */
public class WorkerTest {

	private ActorSystem actorSystem;

	@Before
	public void setUp() {
		this.actorSystem = ActorSystem.create();
	}

	@Test
	public void shouldFindCorrectPrimes() {
		// The double brackets are important! The inner bracket pair declares an anonymous constructor.
		new TestKit(this.actorSystem) {{
			ActorRef worker = actorSystem.actorOf(Worker.props());

			// Send a message to the worker.
			worker.tell(new Worker.ValidationMessage(0, 1, 10), this.getRef());

			// Expect the correct response.
			Master.PrimesMessage expectedMsg = new Master.PrimesMessage(0, Arrays.asList(2L, 3L, 5L, 7L), true);
			this.expectMsg(Duration.create(3, "secs"), expectedMsg);
		}};
	}

	@Test
	public void shouldInterpretRangeCorrectly() {
		// The double brackets are important! The inner bracket pair declares an anonymous constructor.
		new TestKit(this.actorSystem) {{
			ActorRef worker = actorSystem.actorOf(Worker.props());

			// Send a message to the worker.
			worker.tell(new Worker.ValidationMessage(1, 5, 11), this.getRef());

			// Expect the correct response.
			Master.PrimesMessage expectedMsg = new Master.PrimesMessage(1, Arrays.asList(5L, 7L, 11L), true);
			this.expectMsg(Duration.create(3, "secs"), expectedMsg);
		}};
	}

	@After
	public void tearDown() {
		this.actorSystem.terminate();
	}

}
