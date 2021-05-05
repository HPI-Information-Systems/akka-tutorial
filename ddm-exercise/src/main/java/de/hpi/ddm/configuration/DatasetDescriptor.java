package de.hpi.ddm.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import com.opencsv.exceptions.CsvValidationException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor @AllArgsConstructor
public class DatasetDescriptor implements Serializable {
	
	private static final long serialVersionUID = 1985782678973727520L;
	
	private String datasetName = "passwords";
	private String datasetPath = "data" + File.separator;
	private String datasetEnding = ".csv";

	private boolean fileHasHeader = true;
	private Charset charset = StandardCharsets.UTF_8;
	
	private char valueSeparator = ';';
	private char valueQuote = '"';
	private char valueEscape = '\\';
	private boolean valueStrictQuotes = false;
	private boolean valueIgnoreLeadingWhitespace = true;	// Ignore i.e. delete all whitespaces preceding any read value 
	
	private boolean readerSkipDifferingLines = true;		// True if the reader should skip lines in the input that have a different length as the first line
	
	public String getDatasetPathNameEnding() {
		String pathNameSeparator = this.datasetPath.endsWith(File.separator) ? "" : File.separator;
		String nameEndingSeparator = this.datasetEnding.startsWith(".") ? "" : ".";
		
		return this.datasetPath + pathNameSeparator + this.datasetName + nameEndingSeparator + this.datasetEnding;
	}

	public void update(CommandMaster commandMaster) {
		this.datasetName = commandMaster.datasetName;
		this.datasetPath = commandMaster.datasetPath;
		this.datasetEnding = commandMaster.datasetEnding;
		this.fileHasHeader = commandMaster.fileHasHeader;
		this.charset = commandMaster.charset;
		this.valueSeparator = commandMaster.attributeSeparator;
		this.valueQuote = commandMaster.attributeQuote;
		this.valueEscape = commandMaster.attributeEscape;
		this.valueStrictQuotes = commandMaster.attributeStrictQuotes;
		this.valueIgnoreLeadingWhitespace = commandMaster.attributeIgnoreLeadingWhitespace;
		this.readerSkipDifferingLines = commandMaster.readerSkipDifferingLines;
	}

	public CSVReader createCSVReader() throws IOException, CsvValidationException {
		Path path = Paths.get(this.datasetPath + this.datasetName + this.datasetEnding);
		
		CSVParser parser = new CSVParserBuilder()
				.withSeparator(this.valueSeparator)
				.withQuoteChar(this.valueQuote)
				.withEscapeChar(this.valueEscape)
				.withStrictQuotes(this.valueStrictQuotes)
				.withIgnoreLeadingWhiteSpace(this.valueIgnoreLeadingWhitespace)
				.withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
				.build();
		
		BufferedReader buffer = Files.newBufferedReader(path, this.charset);
		CSVReader reader = new CSVReaderBuilder(buffer).withCSVParser(parser).build();
		
		if (this.fileHasHeader)
			reader.readNext();
		
		return reader;
	}
}
