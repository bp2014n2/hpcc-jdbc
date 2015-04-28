package de.hpi.hpcc.main;

import java.util.Iterator;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HPCCNodeListIterator implements Iterator<Node> {

	private NodeList nodeList;
	private int currentIndex = -1;
	
	public HPCCNodeListIterator(NodeList nodeList) {
		this.nodeList = nodeList;
	}

	@Override
	public boolean hasNext() {
		return (nodeList.item(currentIndex+1) != null);
	}

	@Override
	public Node next() {
		return nodeList.item(++currentIndex);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
