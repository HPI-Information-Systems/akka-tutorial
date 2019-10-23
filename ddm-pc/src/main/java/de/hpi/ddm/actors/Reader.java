package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import de.hpi.ddm.configuration.DatasetDescriptorSingleton;
import lombok.Data;

public class Reader extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "reader";

	public static Props props() {
		return Props.create(Reader.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class ReadMessage implements Serializable {
		private static final long serialVersionUID = -3254147511955012292L;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private CSVReader reader;
	
	private int bufferSize;
	
	private List<String[]> buffer;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() throws Exception {
		Reaper.watchWithDefaultReaper(this);
		
		this.reader = DatasetDescriptorSingleton.get().createCSVReader();
		this.bufferSize = ConfigurationSingleton.get().getBufferSize();
		this.buffer = new ArrayList<>(this.bufferSize);
		
		this.read();
	}

	@Override
	public void postStop() throws Exception {
		this.reader.close();
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ReadMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(ReadMessage message) throws Exception {
		this.sender().tell(new Master.BatchMessage(new ArrayList<>(this.buffer)), this.self());
		
		this.read();
	}
	
	private void read() throws Exception {
		this.buffer.clear();
		
		String[] line;
		while ((this.buffer.size() < this.bufferSize) && ((line = this.reader.readNext()) != null))
			this.buffer.add(line);
	}
}
