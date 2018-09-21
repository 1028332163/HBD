package com.esoft.dp.entity;
/**
 * 道路等级表
 * @author taosh
 *
 */
public class IOVRoadType {
	
	private Integer id;
	
	private String roadCode;
	
	private String roadName;
	
	private String start;
	
	private String end;
	
	private String roadLength;
	
	private Integer roadLevel;
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getRoadCode() {
		return roadCode;
	}

	public void setRoadCode(String roadCode) {
		this.roadCode = roadCode;
	}

	public String getRoadName() {
		return roadName;
	}

	public void setRoadName(String roadName) {
		this.roadName = roadName;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public String getRoadLength() {
		return roadLength;
	}

	public void setRoadLength(String roadLength) {
		this.roadLength = roadLength;
	}

	public Integer getRoadLevel() {
		return roadLevel;
	}

	public void setRoadLevel(Integer roadLevel) {
		this.roadLevel = roadLevel;
	}

}
