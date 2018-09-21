package com.esoft.dp.entity;

import java.sql.Timestamp;
/**
 * GPS 表
 * @author taosh
 *
 */
public class ProcGPS {
	
	private Integer recUID;
	
	private String deviceID;
	
	private Double vehicleSpeed;
	
	private Double longitude;
	
	private Timestamp locationTime;
	
	private Double latitude;
	
	private Double direct;
	
	private String roadNameRealStreet;
	
	private String roadNameReal;
	
	private String roadName;
	
	private String roadLevel;
	
	public Integer getRecUID() {
		return recUID;
	}

	public void setRecUID(Integer recUID) {
		this.recUID = recUID;
	}

	public String getDeviceID() {
		return deviceID;
	}

	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
	}

	public Double getVehicleSpeed() {
		return vehicleSpeed;
	}

	public void setVehicleSpeed(Double vehicleSpeed) {
		this.vehicleSpeed = vehicleSpeed;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public Timestamp getLocationTime() {
		return locationTime;
	}

	public void setLocationTime(Timestamp locationTime) {
		this.locationTime = locationTime;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getDirect() {
		return direct;
	}

	public void setDirect(Double direct) {
		this.direct = direct;
	}

	public String getRoadNameRealStreet() {
		return roadNameRealStreet;
	}

	public void setRoadNameRealStreet(String roadNameRealStreet) {
		this.roadNameRealStreet = roadNameRealStreet;
	}

	public String getRoadNameReal() {
		return roadNameReal;
	}

	public void setRoadNameReal(String roadNameReal) {
		this.roadNameReal = roadNameReal;
	}

	public String getRoadName() {
		return roadName;
	}

	public void setRoadName(String roadName) {
		this.roadName = roadName;
	}

	public String getRoadLevel() {
		return roadLevel;
	}

	public void setRoadLevel(String roadLevel) {
		this.roadLevel = roadLevel;
	}


}
