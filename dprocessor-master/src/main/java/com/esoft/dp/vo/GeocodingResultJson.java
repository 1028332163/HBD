package com.esoft.dp.vo;

public class GeocodingResultJson {
	/** 地址详细信息 */
	private AddressComponentJson addressComponent;
	/** 结构化地址信息 */
	private String formatted_address;
	/** 所在商圈信息，如 "人民大学,中关村,苏州街" */
	private String business;
	/** 当前位置结合POI的语义化结果描述。 */
	private String sematic_description;
	private String cityCode;

	/** 经纬度 */
	private class Location {
		private String lng;
		private String lat;
		
		public String getLng() {
			return lng;
		}
		public void setLng(String lng) {
			this.lng = lng;
		}
		public String getLat() {
			return lat;
		}
		public void setLat(String lat) {
			this.lat = lat;
		}
	}

	public AddressComponentJson getAddressComponent() {
		return addressComponent;
	}

	public void setAddressComponent(AddressComponentJson addressComponent) {
		this.addressComponent = addressComponent;
	}

	public String getFormatted_address() {
		return formatted_address;
	}

	public void setFormatted_address(String formatted_address) {
		this.formatted_address = formatted_address;
	}

	public String getBusiness() {
		return business;
	}

	public void setBusiness(String business) {
		this.business = business;
	}

	public String getSematic_description() {
		return sematic_description;
	}

	public void setSematic_description(String sematic_description) {
		this.sematic_description = sematic_description;
	}

	public String getCityCode() {
		return cityCode;
	}

	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}
	
	
}
