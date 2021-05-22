package de.hpi.ddm.actors;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import de.hpi.ddm.actors.Master.HashSolutionMessage;
import de.hpi.ddm.actors.Master.StartMessage;
import de.hpi.ddm.actors.Worker.CrackNMessage;
import de.hpi.ddm.actors.Worker.FinishedPermutationsMessage;
import de.hpi.ddm.actors.Worker.ReadyForMoreMessage;
import de.hpi.ddm.actors.Worker.HintHashesMessage;
import de.hpi.ddm.actors.Worker.InitConfigurationMessage;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.singletons.ConfigurationSingleton;
import de.hpi.ddm.systems.MasterSystem;

public class WorkerCrackingTest {

	static ActorSystem system;

	@Before
	public void setUp() throws Exception {
		final Configuration c = ConfigurationSingleton.get();

		final Config config = ConfigFactory
				.parseString("akka.remote.artery.canonical.hostname = \"" + c.getHost() + "\"\n"
						+ "akka.remote.artery.canonical.port = " + c.getPort() + "\n" + "akka.cluster.roles = ["
						+ MasterSystem.MASTER_ROLE + "]\n" + "akka.cluster.seed-nodes = [\"akka://"
						+ c.getActorSystemName() + "@" + c.getMasterHost() + ":" + c.getMasterPort() + "\"]")
				.withFallback(ConfigFactory.load("application"));

		system = ActorSystem.create(c.getActorSystemName(), config);
	}

	@After
	public void tearDown() throws Exception {
		TestKit.shutdownActorSystem(system);
	}

	@Test
	public void testGeneratingSinglePermutation() {
		new TestKit(system) {
			{
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

				ActorRef worker = system.actorOf(Worker.props(), "worker");
				final TestKit finishCrackingPrope = new TestKit(system);
				worker.tell(new InitConfigurationMessage(new char[] {'a', 'b', 'c'}, 0, 1,	0), finishCrackingPrope.getRef());
				List<String> hashes = new ArrayList<String>();
				hashes.add(Worker.hash("abc"));
				worker.tell(new HintHashesMessage(hashes), finishCrackingPrope.getRef());
				worker.tell(new CrackNMessage(1), finishCrackingPrope.getRef());
				within(Duration.ofSeconds(4), () -> {
					finishCrackingPrope.expectMsgClass(HashSolutionMessage.class);
					finishCrackingPrope.expectMsgClass(FinishedPermutationsMessage.class);
					return null;
				});
			}
		};
	}

	@Test
	public void testGeneratingMultiplePermutations() {
		new TestKit(system) {
			{
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

				ActorRef worker = system.actorOf(Worker.props(), "worker");
				final TestKit finishCrackingPrope = new TestKit(system);
				worker.tell(new InitConfigurationMessage(new char[] {'d', 'e', 'f'},0, 2,	0), finishCrackingPrope.getRef());
				List<String> hashes = new ArrayList<String>();
				hashes.add(Worker.hash("def"));
				worker.tell(new HintHashesMessage(hashes), finishCrackingPrope.getRef());
				worker.tell(new CrackNMessage(1), finishCrackingPrope.getRef());
				within(Duration.ofSeconds(4), () -> {
					finishCrackingPrope.expectMsgClass(HashSolutionMessage.class);
					finishCrackingPrope.expectMsgClass(ReadyForMoreMessage.class);
					finishCrackingPrope.expectNoMessage();
					return null;
				});
			}
		};
	}

	@Test
	public void testCanHandleTooManyPermutations() {
		new TestKit(system) {
			{
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

				ActorRef worker = system.actorOf(Worker.props(), "worker");
				final TestKit finishCrackingPrope = new TestKit(system);
				worker.tell(new InitConfigurationMessage(new char[] {'d', 'e', 'f'},0, 20,	0), finishCrackingPrope.getRef());
				List<String> hashes = new ArrayList<String>();
				hashes.add(Worker.hash("def"));
				worker.tell(new HintHashesMessage(hashes), finishCrackingPrope.getRef());
				worker.tell(new CrackNMessage(20), finishCrackingPrope.getRef());
				within(Duration.ofSeconds(4), () -> {
					finishCrackingPrope.expectMsgClass(HashSolutionMessage.class);
					finishCrackingPrope.expectMsgClass(FinishedPermutationsMessage.class);
					finishCrackingPrope.expectNoMessage();
					return null;
				});
			}
		};
	}


	@Test
	public void testGeneratingMultipleCrackSteps() {
		new TestKit(system) {
			{
				ActorRef reaper = system.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

				ActorRef worker = system.actorOf(Worker.props(), "worker");
				final TestKit finishCrackingPrope = new TestKit(system);
				worker.tell(new InitConfigurationMessage(new char[] {'g', 'h', 'i'}, 0,6,	0), finishCrackingPrope.getRef());
				List<String> hashes = new ArrayList<String>();
				hashes.add(Worker.hash("ghi"));
				hashes.add(Worker.hash("ihg"));
				worker.tell(new HintHashesMessage(hashes), finishCrackingPrope.getRef());
				within(Duration.ofSeconds(4), () -> {
					worker.tell(new CrackNMessage(2), finishCrackingPrope.getRef());
					finishCrackingPrope.expectMsgClass(HashSolutionMessage.class);
					finishCrackingPrope.expectMsgClass(ReadyForMoreMessage.class);
					worker.tell(new CrackNMessage(2), finishCrackingPrope.getRef());
					finishCrackingPrope.expectMsgClass(ReadyForMoreMessage.class);
					worker.tell(new CrackNMessage(2), finishCrackingPrope.getRef());
					finishCrackingPrope.expectMsgClass(HashSolutionMessage.class);
					finishCrackingPrope.expectMsgClass(FinishedPermutationsMessage.class);
					return null;
				});
			}
		};
	}
}
