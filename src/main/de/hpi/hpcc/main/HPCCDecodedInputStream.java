package de.hpi.hpcc.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class HPCCDecodedInputStream extends InputStream {

        private static HashMap<String, Integer> entities = new HashMap<String, Integer>(){{
              put("quot", new Integer((int)'"')); 
              put("amp",  new Integer((int)'&'));
              put("lt",   new Integer((int)'<'));
              put("gt",   new Integer((int)'>'));
              put("tild", new Integer((int)'~'));
              put("apos", new Integer((int)'\''));
        }};

        private InputStream input;

        public HPCCDecodedInputStream(InputStream input) {
              super();
              this.input = input; 
        }

        public void setInput(InputStream input) {
              this.input = input;
        }

	@Override
	public int read() throws IOException {
            if(input == null) {
                throw new IOException("HPCCDecodedInputStream: call setInput(InputStream input) with the source input stream before reading!");
            }
            int nextByte = input.read();
            if((char) nextByte == '&') {
                String encoded = "";
                while((char)(nextByte = input.read()) != ';'){
                    encoded += (char) nextByte; 
                }
                return entities.get(encoded).intValue();
            }
	    return nextByte;
	}

}
