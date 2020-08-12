package com.example.postgresql;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

public class JobSkipPolicy implements SkipPolicy{

	public boolean shouldSkip(Throwable throwable, int failedCount) throws SkipLimitExceededException {
		// TODO Auto-generated method stub
		return true;
	}

	

}
