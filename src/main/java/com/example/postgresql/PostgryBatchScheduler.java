package com.example.postgresql;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.ReferenceJobFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.launch.NoSuchJobInstanceException;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PostgryBatchScheduler {
	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job job;

	// @Autowired
	// JobOperator jobOperator;

	@Autowired
	JobExplorer jobExplorer;

	@Autowired
	JobRegistry jobRegistry;

	@Autowired
	JobRepository jobRepository;

	@Scheduled(cron = "*/10 * * * * *")
	public void jobScheduled() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, DuplicateJobException,
			NoSuchJobException, NoSuchJobInstanceException, NoSuchJobExecutionException {

		JobExecution run = null;
		int f = 0;

		ReferenceJobFactory referenceJobFactory = new ReferenceJobFactory(job);
		jobRegistry.register(referenceJobFactory);

		SimpleJobOperator jobOperator = new SimpleJobOperator();
		jobOperator.setJobLauncher(jobLauncher);
		jobOperator.setJobRepository(jobRepository);
		jobOperator.setJobRegistry(jobRegistry);
		jobOperator.setJobExplorer(jobExplorer);

		List<Long> jobInstances = jobOperator.getJobInstances("CSV-LOAD", 0, 10);
		if (!jobInstances.isEmpty()) {
			Long instanceId = jobInstances.get(0);
			List<Long> executions = jobOperator.getExecutions(instanceId);
			if (!executions.isEmpty()) {
				Long executionId = executions.get(0);
				System.err.println("EXECUTIONID" + executionId);
				JobExecution jobExecution = jobExplorer.getJobExecution(executionId);

				System.err.println("TopStatus" + jobExecution.getStatus());
				if (jobExecution.getStatus().equals(BatchStatus.STARTED)) {
					System.err.println("INSIDE CHANGING");
					f = 1;
					jobExecution.setStatus(BatchStatus.FAILED);
					jobExecution.setEndTime(new Date());
					jobRepository.update(jobExecution);

					System.err.println("insideStatus" + jobExecution.getStatus());

					Long restartId = jobOperator.restart(executionId);
					System.err.println("RESTARTID" + restartId);
					run = jobExplorer.getJobExecution(restartId);
				}

				System.err.println("OUTSIDEStatus" + jobExecution.getStatus());

				if (jobExecution.getStatus().equals(BatchStatus.FAILED) && f == 0) {
					try {
						System.err.println("RESUME");
						JobParametersBuilder builder = new JobParametersBuilder();
						builder.addDate("date", new Date());
						Long restartId = jobOperator.restart(executionId);
						System.err.println("RESTARTID" + restartId);
						run = jobExplorer.getJobExecution(restartId);
					} catch (Exception e) {
						// LOG.error("Error resuming job " + executionId + ", a new job instance will be
						// created. Cause: " + e.getLocalizedMessage());
					}
				}
			}
		}

		if (run == null) {
			System.err.println("NEW");
			JobParametersBuilder builder = new JobParametersBuilder();
			builder.addDate("date", new Date());
			
//			Map<String, JobParameter> maps = new HashMap<>();
//			maps.put("time", new JobParameter(System.currentTimeMillis()));
//			JobParameters parameters = new JobParameters(maps);
			run = jobLauncher.run(job, builder.toJobParameters());
		}

	}

}


















// @Scheduled(cron="*/10 * * * * *")
// public void jobScheduled() throws JobExecutionAlreadyRunningException,
// JobRestartException, JobInstanceAlreadyCompleteException,
// JobParametersInvalidException{
// System.err.println("BATCH");
// File file = new
// File("C:/Users/ELCOT/Desktop/chunkresume1/src/main/resources/sales5000.csv");
//
// System.err.println(file.exists());
// if(file.exists()) {
// Map<String,JobParameter>maps = new HashMap<>();
// maps.put("time", new JobParameter(System.currentTimeMillis()));
// JobParameters parameters=new JobParameters(maps);
//
// JobExecution jobExecution = jobLauncher.run(job, parameters);
// System.out.println("JOB EXECUTION :" + jobExecution.getStatus());
// if(!jobExecution.getStatus().isUnsuccessful()) {
// file.delete();
// System.err.println("DELETED");
// }
// }
// List<JobInstance> instances = jobExplorer.getJobInstances("job",0,1);
// System.err.println("Exploler Size : " + instances.size());
// for(JobInstance instance:instances) {
//
// List<JobExecution> executions = jobExplorer.getJobExecutions(instance);
// System.err.println("Executions size : " + executions.size());
// if(executions.size() > 0) {
// JobExecution jobExecution = executions.get(executions.size() - 1);
// if(jobExecution.getStatus().name().equals("FAILED")) {
// long a= jobOperator.restart(instance.getId(),null);
// System.out.println("Exit Status : " + a);
// }
// }
// }
// }
//
//
