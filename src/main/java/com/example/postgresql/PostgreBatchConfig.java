package com.example.postgresql;

import java.sql.SQLIntegrityConstraintViolationException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class PostgreBatchConfig {

	@Autowired
	private DataSource dataSource;

	  @Autowired
	  private PlatformTransactionManager platformTransactionManager;
	  
	  private static final String ISOLATION_REPEATABLE_READ = "ISOLATION_REPEATABLE_READ";
	  
	  
	@Bean
	public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
			ItemWriter<Sales> itemWritter, ItemProcessor<Sales, Sales> itemProcessor, ItemReader<Sales> itemReader)

	{
		@SuppressWarnings("unchecked")
		SimpleStepBuilder<Sales, Sales> simpleStepBuilder = (SimpleStepBuilder<Sales, Sales>) stepBuilderFactory.get("CSV-FILE-LOAD").<Sales, Sales>chunk(10000).reader(itemReader)
				.processor(processor()).writer(itemWritter)
				.taskExecutor(taskExecutor());
		Step step = simpleStepBuilder
				.faultTolerant().skipPolicy(skipPolicy(IllegalArgumentException.class))
				.build();

		return jobBuilderFactory.get("CSV-LOAD")
				 .incrementer(new RunIdIncrementer())
				.start(step).build();

	}
	
	
	@Bean
	JobSkipPolicy skipPolicy(Class<IllegalArgumentException> class1) {
		return new JobSkipPolicy();
	}
	
	
	@Bean
	  public JobRepository jobRepository() throws Exception {
	    JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
	    factory.setDataSource(dataSource);
	    factory.setTransactionManager(platformTransactionManager);
	    factory.setValidateTransactionState(true);
	    factory.setIsolationLevelForCreate(ISOLATION_REPEATABLE_READ);
	   // factory.setIncrementerFactory(customIncrementerFactory());
	    factory.afterPropertiesSet();
	    return factory.getObject();
	  }
	
	
	@Bean
	public JobRegistry jobRegistry() throws Exception {
		MapJobRegistry jobRegistry;
		return jobRegistry = new MapJobRegistry();
	}
	
	  @Bean
	  public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
	    SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
	    simpleJobLauncher.setJobRepository(jobRepository);
	    return simpleJobLauncher;
	  }
	  

	@Bean
	public JobExplorer jobExplorer() throws Exception {
		System.err.println("EXPLORER");
		final JobExplorerFactoryBean bean = new JobExplorerFactoryBean();
		bean.setDataSource(dataSource);
		bean.setTablePrefix("BATCH_");
		bean.setJdbcOperations(new JdbcTemplate(dataSource));
		bean.afterPropertiesSet();
		return bean.getObject();
	}

	@Bean
	public ItemProcessor<Sales, Sales> processor() {
		return new Processor();
	}

	@Bean
	public FlatFileItemReader<Sales> itemReader() {
		FlatFileItemReader<Sales> flatFileItemReader = new FlatFileItemReader<>();
		flatFileItemReader.setResource(
				new FileSystemResource("C:/Users/ELCOT/Desktop/postgresql/src/main/resources/salesFt.csv"));
		flatFileItemReader.setName("CSV_READER");
		flatFileItemReader.setLinesToSkip(1);
		flatFileItemReader.setLineMapper(lineMapper());
		return flatFileItemReader;
	}

	@Bean
	public LineMapper<Sales> lineMapper() {
		DefaultLineMapper<Sales> defaultLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTockenizer = new DelimitedLineTokenizer();

		lineTockenizer.setDelimiter(",");
		lineTockenizer.setStrict(false);
		defaultLineMapper.setLineTokenizer(lineTockenizer);
		defaultLineMapper.setFieldSetMapper(new SalesFieldSetMapper());

		return defaultLineMapper;
	}

	@Bean
	public JdbcBatchItemWriter<Sales> itemWriter(DataSource dataSource) {

		return new JdbcBatchItemWriterBuilder<Sales>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO salesrecord (orderid,region,country,itemtype,saleschannel,orderpriority,orderdate,shipdate,unitssold,unitprice,unitcost,totalrevenue,totalcost,totalprofit) VALUES (:orderId, :region, :country, :itemType, :salesChannel, :orderPriority, :orderDate, :shipDate, :unitsSold, :unitPrice, :unitCost, :totalRevenue, :totalCost, :totalProfit)")
				.dataSource(dataSource).build();
		// System.err.println("AFTER");
	}

	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(10);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	
//    @Bean
//    public TaskExecutor taskExecutor() {
//        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor("spring_batch");
//        asyncTaskExecutor.setConcurrencyLimit(4);
//        return asyncTaskExecutor;
//    }
    
}
