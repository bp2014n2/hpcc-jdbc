package de.hpi.hpcc.main;

import java.util.Iterator;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HPCCNodeListAdapter implements Iterable<Node> {

	private NodeList nodeList;

	public HPCCNodeListAdapter(NodeList nodeList) {
		this.nodeList = nodeList;
	}

	@Override
	public Iterator<Node> iterator() {
		return new HPCCNodeListIterator(nodeList);
	}

}
