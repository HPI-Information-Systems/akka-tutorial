package de.hpi.ddm.configuration;

import java.nio.charset.Charset;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import de.hpi.ddm.singletons.ConfigurationSingleton;
import de.hpi.ddm.singletons.DatasetDescriptorSingleton;

@Parameters(commandDescription = "start a master actor system")
public class CommandMaster extends Command {

	@Override
	int getDefaultPort() {
		return Configuration.DEFAULT_MASTER_PORT;
	}

	@Parameter(names = { "-sp", "--startPaused" }, description = "Wait for some console input to start the discovery; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)", required = false, arity = 1)
	boolean startPaused = ConfigurationSingleton.get().isStartPaused();

	@Parameter(names = { "-bs", "--bufferSize" }, description = "Buffer for input reading (the DatasetReader pre-fetches and buffers this many records)", required = false)
	int bufferSize = ConfigurationSingleton.get().getBufferSize();

	@Parameter(names = { "-wms", "--welcomeMessageSize" }, description = "Size of the welcome message's data (in MB) with which each worker should be greeted.", required = false)
	int welcomeDataSize = ConfigurationSingleton.get().getWelcomeDataSize();
	
	// DatasetDescriptor
	
	@Parameter(names = { "-dn", "--datasetName" }, description = "Dataset name", required = false)
	String datasetName = DatasetDescriptorSingleton.get().getDatasetName();

	@Parameter(names = { "-dp", "--datasetPath" }, description = "Dataset path", required = false)
	String datasetPath = DatasetDescriptorSingleton.get().getDatasetPath();

	@Parameter(names = { "-de", "--datasetEnding" }, description = "Dataset ending", required = false)
	String datasetEnding = DatasetDescriptorSingleton.get().getDatasetEnding();

	@Parameter(names = { "-fh", "--fileHasHeader" }, description = "File has header as defined by the input data", required = false, arity = 1)
	boolean fileHasHeader = DatasetDescriptorSingleton.get().isFileHasHeader();

	@Parameter(names = { "-cs", "--charset" }, description = "Charset as defined by the input data", required = false)
	Charset charset = DatasetDescriptorSingleton.get().getCharset();

	@Parameter(names = { "-vs", "--valueSeparator" }, description = "Value separator as defined by the input data", required = false)
	char attributeSeparator = DatasetDescriptorSingleton.get().getValueSeparator();

	@Parameter(names = { "-vq", "--valueQuote" }, description = "Value quote as defined by the input data", required = false)
	char attributeQuote = DatasetDescriptorSingleton.get().getValueQuote();

	@Parameter(names = { "-ve", "--valueEscape" }, description = "Value escape as defined by the input data", required = false)
	char attributeEscape = DatasetDescriptorSingleton.get().getValueEscape();

	@Parameter(names = { "-vsq", "--valueStrictQuotes" }, description = "Value strict quotes as defined by the input data", required = false, arity = 1)
	boolean attributeStrictQuotes = DatasetDescriptorSingleton.get().isValueStrictQuotes();

	@Parameter(names = { "-viw", "--valueIgnoreLeadingWhitespace" }, description = "Ignore i.e. delete all whitespaces preceding any read value ", required = false, arity = 1)
	boolean attributeIgnoreLeadingWhitespace = DatasetDescriptorSingleton.get().isValueIgnoreLeadingWhitespace();

	@Parameter(names = { "-rsdl", "--readerSkipDifferingLines" }, description = "True if the reader should skip lines in the input that have a different length as the first line", required = false, arity = 1)
	boolean readerSkipDifferingLines = DatasetDescriptorSingleton.get().isReaderSkipDifferingLines();
}
