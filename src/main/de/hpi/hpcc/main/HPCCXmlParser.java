package de.hpi.hpcc.main;

import java.io.InputStream;
import java.util.LinkedList;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class HPCCXmlParser {	
	public LinkedList<LinkedList<String>> parseDataset(InputStream xml) throws HPCCException {
		//TODO track time
		LinkedList<LinkedList<String>> rows = new LinkedList<LinkedList<String>>();
		try {
			XMLStreamReader parser = XMLInputFactory.newInstance().createXMLStreamReader(xml);
			loop: for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						if (this.isRow(parser.getLocalName())) {
							rows.add(this.parseRow(parser));
				        } else if (this.isException(parser.getLocalName())) {
				        	this.parseException(parser);
						}
						break;
					case XMLStreamConstants.END_ELEMENT:
						if (this.isDataset(parser.getLocalName())) {
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
			return rows;
		} catch (XMLStreamException | FactoryConfigurationError e1) {
			throw new HPCCException("Error creating the XML parser!");
		}
    }

	private LinkedList<String> parseRow(XMLStreamReader parser) throws XMLStreamException, HPCCException {
		LinkedList<String> row = new LinkedList<String>();
		String elementValue = "";
		for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
			switch (event) {
				case XMLStreamConstants.CHARACTERS:
				case XMLStreamConstants.CDATA:
					elementValue += parser.getText();
					break;
				case XMLStreamConstants.END_ELEMENT:
					if (this.isRow(parser.getLocalName())) {
						return row;
					}
					row.add(elementValue);
					elementValue = "";
			}
		}
		throw new HPCCException("Error in parsing rows!");
	}

	private void parseException(XMLStreamReader parser) throws XMLStreamException, HPCCException {
		String exceptionMessage = "";
		for (int event = parser.next(); event != XMLStreamConstants.END_ELEMENT; event = parser.next()) {
			switch (event) {
				case XMLStreamConstants.CHARACTERS:
				case XMLStreamConstants.CDATA:
					exceptionMessage += parser.getText();
			}
		}
		throw new HPCCException(HPCCXmlParser.class.getSimpleName()+": Error in server response!!\n"+exceptionMessage);
	}
	
	private boolean isRow(String localName) {
		return localName.equals("Row");
	}
	
	private boolean isException(String localName) {
		return localName.equals("Exception");
	}
	
	private boolean isDataset(String localName) {
		return localName.equals("Dataset");
	}
}
