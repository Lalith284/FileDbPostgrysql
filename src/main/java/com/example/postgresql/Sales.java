package com.example.postgresql;

import java.util.Date;

public class Sales {
	
	
Integer orderId;
	
	String region;
	

	String country; 
	

	String itemType;
	

	String salesChannel;
	

	String orderPriority;
	

	Date orderDate;
	

	Date shipDate;
	

	Integer unitsSold;
	
	
	Float unitPrice;
	

	Float unitCost;
	

	Float totalRevenue;
	

	Float totalCost;

	Float totalProfit;

	public Sales() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Sales(Integer orderId, String region, String country, String itemType, String salesChannel,
			String orderPriority, Date orderDate, Date shipDate, Integer unitsSold, Float unitPrice, Float unitCost,
			Float totalRevenue, Float totalCost, Float totalProfit) {
		super();
		this.orderId = orderId;
		this.region = region;
		this.country = country;
		this.itemType = itemType;
		this.salesChannel = salesChannel;
		this.orderPriority = orderPriority;
		this.orderDate = orderDate;
		this.shipDate = shipDate;
		this.unitsSold = unitsSold;
		this.unitPrice = unitPrice;
		this.unitCost = unitCost;
		this.totalRevenue = totalRevenue;
		this.totalCost = totalCost;
		this.totalProfit = totalProfit;
	}

	public Integer getOrderId() {
		return orderId;
	}

	public void setOrderId(Integer orderId) {
		this.orderId = orderId;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getItemType() {
		return itemType;
	}

	public void setItemType(String itemType) {
		this.itemType = itemType;
	}

	public String getSalesChannel() {
		return salesChannel;
	}

	public void setSalesChannel(String salesChannel) {
		this.salesChannel = salesChannel;
	}

	public String getOrderPriority() {
		return orderPriority;
	}

	public void setOrderPriority(String orderPriority) {
		this.orderPriority = orderPriority;
	}

	public Date getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(Date orderDate) {
		this.orderDate = orderDate;
	}

	public Date getShipDate() {
		return shipDate;
	}

	public void setShipDate(Date shipDate) {
		this.shipDate = shipDate;
	}

	public Integer getUnitsSold() {
		return unitsSold;
	}

	public void setUnitsSold(Integer unitsSold) {
		this.unitsSold = unitsSold;
	}

	public Float getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Float unitPrice) {
		this.unitPrice = unitPrice;
	}

	public Float getUnitCost() {
		return unitCost;
	}

	public void setUnitCost(Float unitCost) {
		this.unitCost = unitCost;
	}

	public Float getTotalRevenue() {
		return totalRevenue;
	}

	public void setTotalRevenue(Float totalRevenue) {
		this.totalRevenue = totalRevenue;
	}

	public Float getTotalCost() {
		return totalCost;
	}

	public void setTotalCost(Float totalCost) {
		this.totalCost = totalCost;
	}

	public Float getTotalProfit() {
		return totalProfit;
	}

	public void setTotalProfit(Float totalProfit) {
		this.totalProfit = totalProfit;
	}

	@Override
	public String toString() {
		return "Sales [orderId=" + orderId + ", region=" + region + ", country=" + country + ", itemType=" + itemType
				+ ", salesChannel=" + salesChannel + ", orderPriority=" + orderPriority + ", orderDate=" + orderDate
				+ ", shipDate=" + shipDate + ", unitsSold=" + unitsSold + ", unitPrice=" + unitPrice + ", unitCost="
				+ unitCost + ", totalRevenue=" + totalRevenue + ", totalCost=" + totalCost + ", totalProfit="
				+ totalProfit + "]";
	}
	
	
	
	


}
