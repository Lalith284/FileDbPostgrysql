package com.example.postgresql;

import org.springframework.batch.item.ItemProcessor;

public class Processor implements ItemProcessor<Sales,Sales>{
private int k=0;
	@Override
	public Sales process(Sales sales) throws Exception {
		// TODO Auto-generated method stub
		k++;
		if(k==20000) {
			throw new Exception("Exception created and program stopped");
		}
		sales.setCountry("INDIA");
		return sales;
	}

}
