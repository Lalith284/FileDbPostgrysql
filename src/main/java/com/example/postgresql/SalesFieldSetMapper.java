package com.example.postgresql;


import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;




public class SalesFieldSetMapper  implements FieldSetMapper<Sales>{

	@Override
	public Sales mapFieldSet(FieldSet fieldSet) throws BindException {
Sales sales=new Sales();
		
		sales.setOrderId(fieldSet.readInt(6));
		sales.setRegion(fieldSet.readString(0));
		sales.setCountry(fieldSet.readString(1));
		sales.setItemType(fieldSet.readString(2));
		sales.setSalesChannel(fieldSet.readString(3));
		sales.setOrderPriority(fieldSet.readString(4));
		sales.setOrderDate(fieldSet.readDate(5,"MM/dd/yyyy"));
		sales.setShipDate(fieldSet.readDate(7,"MM/dd/yyyy"));
		sales.setUnitsSold(fieldSet.readInt(8));
		sales.setOrderId(fieldSet.readInt(6));
		sales.setUnitPrice(fieldSet.readFloat(9));
		sales.setUnitCost(fieldSet.readFloat(10));
		sales.setTotalRevenue(fieldSet.readFloat(11));
		sales.setTotalCost(fieldSet.readFloat(12));
		return sales;
	}

	}

