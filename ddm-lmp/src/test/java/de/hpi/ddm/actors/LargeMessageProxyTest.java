package de.hpi.ddm.actors;

import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import de.hpi.ddm.MasterSystem;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;

public class LargeMessageProxyTest {

	static ActorSystem system;

	static class TestActor extends AbstractLoggingActor {

		public static Props props(ActorRef parent) {
			return Props.create(TestActor.class, () -> new TestActor(parent));
		}

		public TestActor(ActorRef parent) {
			this.parent = parent;
		}

		ActorRef parent = null;
		ActorRef largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		
		@Override
		public Receive createReceive() {
			return receiveBuilder()
					.match(LargeMessageProxy.LargeMessage.class, message -> this.largeMessageProxy.tell(message, this.self()))
					.match(Object.class, message -> this.parent.tell(message, this.self()))
					.build();
		}
	}

	@Before
	public void setUp() throws Exception {
		final Configuration c = ConfigurationSingleton.get();
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.artery.canonical.hostname = \"" + c.getHost() + "\"\n" +
				"akka.remote.artery.canonical.port = " + c.getPort() + "\n" +
				"akka.cluster.roles = [" + MasterSystem.MASTER_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + c.getActorSystemName() + "@" + c.getMasterHost() + ":" + c.getMasterPort() + "\"]")
			.withFallback(ConfigFactory.load("application"));
		
		system = ActorSystem.create(c.getActorSystemName(), config);
	}

	@After
	public void tearDown() throws Exception {
		TestKit.shutdownActorSystem(system);
	}

	@Test
	public void testSmallMessageSending() {
		new TestKit(system) {
			{
				ActorRef sender = system.actorOf(TestActor.props(this.getRef()), "sender");
				ActorRef receiver = system.actorOf(TestActor.props(this.getRef()), "receiver");
				
				within(Duration.ofSeconds(1), () -> {
					// Test if a small message gets passed from one proxy to the other
					String shortMessage = "Hello, this is a short message!";
					LargeMessageProxy.LargeMessage<String> shortStringMessage = new LargeMessageProxy.LargeMessage<String>(shortMessage, receiver);
					
					sender.tell(shortStringMessage, this.getRef()); // Tell the TestActor to send a large message via its large message proxy to the receiver
					this.expectMsg(shortMessage);
					assertTrue(this.getLastSender().equals(receiver));
					
					// Will wait for the rest of the within duration
					expectNoMessage();
					return null;
				});
			}
		};
	}
	
	@Test
	public void testLargeMessageSending() {
		new TestKit(system) {
			{
				ActorRef sender = system.actorOf(TestActor.props(this.getRef()), "sender");
				ActorRef receiver = system.actorOf(TestActor.props(this.getRef()), "receiver");
				
				within(Duration.ofSeconds(2), () -> {
					// Test if a large message gets passed from one proxy to the other
					StringBuffer longMessageBuffer = new StringBuffer("Hello, this is a String message with a very large payload!");
					for (int i = 0; i < 1000; i++)
						longMessageBuffer.append("<content>");
					String longMessage = longMessageBuffer.toString();
					LargeMessageProxy.LargeMessage<String> longStringMessage = new LargeMessageProxy.LargeMessage<String>(longMessage, receiver);
					
					sender.tell(longStringMessage, this.getRef()); // Tell the TestActor to send a large message via its large message proxy to the receiver
					this.expectMsg(longMessage);
					assertTrue(this.getLastSender().equals(receiver));
					
					// Will wait for the rest of the within duration
					expectNoMessage();
					return null;
				});
			}
		};
	}
}
