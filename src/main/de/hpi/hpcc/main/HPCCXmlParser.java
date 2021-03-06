package de.hpi.hpcc.main;

import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class HPCCXmlParser {
	private XMLStreamReader 		parser;
	private HPCCResultSetMetadata	resultSetMetaData;
	public HPCCXmlParser(InputStream xml, HPCCResultSetMetadata resultSetMetaData, int outputCount) throws HPCCException {
		try {
            HPCCDecodedInputStream xmlEncoded = new HPCCDecodedInputStream(xml);
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			this.parser = inputFactory.createXMLStreamReader(xmlEncoded);
		} catch (XMLStreamException | FactoryConfigurationError e) {
			throw new HPCCException("Error creating the XML parser!");
		}
		this.resultSetMetaData = resultSetMetaData;
		this.setPointerToNeededDataset(outputCount);
	}
	
	private void setPointerToNeededDataset(int outputCount) throws HPCCException {
		//TODO how to deal with exceptions before the needed dataset?
		try {
			for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
				String localName;
				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						localName = parser.getLocalName();
						if (this.isDataset(localName)) {
							if (outputCount == 1) {
								return;
							}
							outputCount--;
						}
						break;
				}
			}
		} catch (XMLStreamException | FactoryConfigurationError e1) {
			throw new HPCCException("Error creating the XML parser!");
		}
	}

	public ArrayList<String> parseNextRow() throws HPCCException {
		//TODO track time
		//TODO use logger :-D
		
		try {
			loop: for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
				String localName;
				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						localName = parser.getLocalName();
						if (this.isRow(localName)) {
							return this.parseRow(resultSetMetaData);
					    } else if (this.isException(parser.getLocalName())) {
					      	this.parseException(parser);
						}
						break;
					case XMLStreamConstants.END_ELEMENT:
						localName = parser.getLocalName();
						if (this.isDataset(localName)) {
							/*
							*	We are only parsing the first dataset!
							*	Additionally, it is possible that the dataset itself is the root element.
							*	So please do not return the rows here! Just break the loop!
							*/
							break loop;
						}	
				}
			}
			parser.close();
			return null;
		} catch (XMLStreamException | FactoryConfigurationError e1) {
			throw new HPCCException("Error creating the XML parser!");
		}
    }

	private ArrayList<String> parseRow(HPCCResultSetMetadata resultSetMetaData) throws XMLStreamException, HPCCException {
		ArrayList<String> row = resultSetMetaData.createDefaultResultRow();
		String elementValue = "";
		String nodeElementName = "";
		for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
			switch (event) {
				case XMLStreamConstants.START_ELEMENT:
					nodeElementName = parser.getLocalName();
					break;
				case XMLStreamConstants.CHARACTERS:
				case XMLStreamConstants.CDATA:
					elementValue += parser.getText();
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (this.isRow(parser.getLocalName())) {
						return row;
					}
					HPCCColumnMetaData columnMetaData = resultSetMetaData.getColByNameOrAlias(nodeElementName);
					row.set(columnMetaData.getIndex(), elementValue);
					elementValue = "";
			}
		}
		throw new HPCCException("Error in parsing rows!");
	}

	private void parseException(XMLStreamReader parser) throws XMLStreamException, HPCCException {
		for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
			String localName;
			switch (event) {
				case XMLStreamConstants.START_ELEMENT:
					localName = parser.getLocalName();
					if (this.isMessage(localName)) {
						throw new HPCCException(HPCCXmlParser.class.getSimpleName()+": Error in server response!!\n"+this.parseMessage());
					}
			}
		}
	}
	
	private String parseMessage() throws XMLStreamException {
		String exceptionMessage  = "";
		for (int event = parser.next(); event != XMLStreamConstants.END_ELEMENT; event = parser.next()) {
			switch (event) {
				case XMLStreamConstants.CHARACTERS:
				case XMLStreamConstants.CDATA:
					exceptionMessage += parser.getText().trim();
			}
		}
		return exceptionMessage;
	}

	private boolean isMessage(String localName) {
		return localName.equals("Message");
	}

	private boolean isRow(String localName) {
		return localName.equals("Row");
	}
	
	private boolean isException(String localName) {
		return localName.equals("Errors");
	}
	
	private boolean isDataset(String localName) {
		return localName.equals("Dataset");
	}
}
