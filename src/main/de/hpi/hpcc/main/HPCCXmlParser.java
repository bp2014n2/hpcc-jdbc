package de.hpi.hpcc.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.logging.Level;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HPCCXmlParser {	
	public NodeList parseDataset(InputStream xml, long startTime) throws HPCCException {
		LinkedList<LinkedList<String>> rows = new LinkedList<LinkedList<String>>();
		try {
			XMLStreamReader parser = XMLInputFactory.newInstance().createXMLStreamReader(xml);
			for (int event = parser.next(); event != XMLStreamConstants.END_DOCUMENT; event = parser.next()) {
				switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						if (isRow(parser.getLocalName())) {
							rows.add(parseRow(parser));
				        } else if (isException(parser.getLocalName())) {
				        	parseException(parser);
						}
						break;
					case XMLStreamConstants.END_ELEMENT:
						if (isDataset(parser.getLocalName())) {
							// TODO we are only parsing the first dataset!
							// TODO return rows;
						}	
				}
			}
			parser.close();
		} catch (XMLStreamException | FactoryConfigurationError e1) {
			throw new HPCCException("Error creating the XML parser!");
		}
		
    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	String expectedDSName = null;
        NodeList rowList = null;

        DocumentBuilder db;
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new HPCCException("Failed to create DocumentBuilder");
		}
        Document dom;
		try {
			dom = db.parse(xml);
		} catch (SAXException | IOException e) {
			throw new HPCCException("Failed to parse dataset");
		}

        long elapsedTime = System.currentTimeMillis() - startTime;

        HPCCJDBCUtils.traceoutln(Level.INFO, "Total elapsed http request/response time in milliseconds: " + elapsedTime);

        Element docElement = dom.getDocumentElement();

        NodeList dsList = docElement.getElementsByTagName("Dataset");

        HPCCJDBCUtils.traceoutln(Level.INFO, "Parsing results...");

        HPCCNodeListAdapter nodes = new HPCCNodeListAdapter(dsList);
        
        int dsCount = 0;
        if (dsList != null && nodes.iterator().hasNext()){
//            HPCCJDBCUtils.traceoutln(Level.INFO, "Results datsets found: " + dsList.getLength());

            // The dataset element is encapsulated within a Result element
            // need to fetch appropriate resulst dataset
            for (Node node : nodes) {
            //for (int i = 0; i < dsCount; i++) {
            
               // Element ds = (Element) dsList.item(i);
            	Element ds = (Element) node;
                String currentdatsetname = ds.getAttribute("name");
                if (expectedDSName == null || expectedDSName.length() == 0
                        || currentdatsetname.equalsIgnoreCase(expectedDSName))
                {
                    rowList = ds.getElementsByTagName("Row");
                    break;
                }
            }
        }
        else if (docElement.getElementsByTagName("Exception").getLength() > 0)
        {
            NodeList exceptionlist = docElement.getElementsByTagName("Exception");

            if (exceptionlist.getLength() > 0)
            {
                HPCCException resexception = null;
                NodeList currexceptionelements = exceptionlist.item(0).getChildNodes();

                for (int j = 0; j < currexceptionelements.getLength(); j++)
                {
                    Node exceptionelement = currexceptionelements.item(j);
                    if (exceptionelement.getNodeName().equals("Message"))
                    {
                        resexception = new HPCCException("HPCCJDBC: Error in response: \'"
                                + exceptionelement.getTextContent() + "\'");
                    }
                }
                if (dsList == null || dsList.getLength() <= 0)
                    throw resexception;
            }
        }
        else
        {
            // The root element is itself the Dataset element
            if (dsCount == 0)
            {
                rowList = docElement.getElementsByTagName("Row");
            }
        }
        HPCCJDBCUtils.traceoutln(Level.INFO,  "Finished Parsing results.");

        return rowList;
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
					if (isRow(parser.getLocalName())) {
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
		throw new HPCCException(exceptionMessage);
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
