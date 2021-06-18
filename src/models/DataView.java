package models;

import java.io.Serializable;

public class DataView implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private long timeCreate;
	private long guid;
	
	public DataView(long timeCreate, long guid) {
		this.timeCreate = timeCreate;
		this.guid = guid;
	}

	public long getTimeCreate() {
		return timeCreate;
	}
	
	public void setTimeCreate(long timeCreate) {
		this.timeCreate = timeCreate;
	}
	
	public long getGuid() {
		return guid;
	}
	
	public void setGuid(long guid) {
		this.guid = guid;
	}
	
	@Override
	public String toString() {
		return timeCreate + " " + guid;
	}
}
