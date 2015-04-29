package de.hpi.hpcc.main;

import java.sql.SQLException;

public class HPCCException extends SQLException {

	private static final long serialVersionUID = 1L;

	public HPCCException() {
		
	}

	public HPCCException(String reason) {
		super(reason);
	}

	public HPCCException(Throwable cause) {
		super(cause);
	}

	public HPCCException(String reason, Throwable cause) {
		super(reason, cause);
	}
	
	public HPCCException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public HPCCException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public HPCCException(String reason, String SQLState, int vendorCode, Throwable cause) {
		super(reason, SQLState, vendorCode, cause);
	}

	public HPCCException(String reason, String SQLState, Throwable cause) {
		super(reason, SQLState, cause);
	}

}
