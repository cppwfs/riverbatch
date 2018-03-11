/*
 * Copyright 2017 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.spring.riverbatch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
@EnableBatchProcessing
@EnableTask
public class RiverConfiguration {

	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Autowired
	DataSource dataSource;

	@Autowired
	ApplicationContext context;

	@Bean
	public Job riverJob() {
		return this.jobBuilderFactory.get("riverJob")
				.start(riverLoad())
				.next(riverProcess())
				.build();
	}

	@Bean
	public Step riverLoad() {
		return this.stepBuilderFactory.get("riverLoad")
				.<String, String>chunk(10)
				.reader(riverReader())
				.writer(dbRiverWriter())
				.build();
	}


	@Bean
	public FlatFileItemReader riverReader() {
		FlatFileItemReader<RiverStat> reader = new FlatFileItemReaderBuilder<RiverStat>()
				.name("riverFileReader")
				.resource(getResource())
				.delimited()
				.delimiter("|")
				.names(new String[]{"dateCaptured", "height"})
				.targetType(RiverStat.class)
				.build();

		reader.open(new ExecutionContext());
		return reader;
	}

	@Bean
	public JdbcBatchItemWriter<RiverStat> dbRiverWriter() {
		JdbcBatchItemWriter<RiverStat> writer = new JdbcBatchItemWriterBuilder<RiverStat>()
				.beanMapped()
				.dataSource(this.dataSource)
				.sql("INSERT INTO riverhistory (dateCaptured, height) VALUES (:dateCaptured, :height)")
				.build();
		return writer;
	}

	@Bean
	public Step riverProcess() {
		return this.stepBuilderFactory.get("riverProcess")
				.startLimit(3)
				.<RiverStat, RiverStat>chunk(5)
				.reader(dbRiverStatReader())
				.writer(riverItemWriter())
				.build();
	}

	@Bean
	public JdbcCursorItemReader<RiverStat> dbRiverStatReader() {
		JdbcCursorItemReader<RiverStat> reader = new JdbcCursorItemReaderBuilder<RiverStat>()
				.dataSource(this.dataSource)
				.name("riverReader")
				.sql("SELECT * FROM riverhistory order by dateCaptured")
				.rowMapper((rs, rowNum) -> {
					RiverStat riverStat = new RiverStat();

					riverStat.setDateCaptured(rs.getString("dateCaptured"));
					riverStat.setHeight(rs.getLong("height"));

					return riverStat;
				})
				.build();
		return reader;
	}

	@Bean
	public ItemWriter<RiverStat> riverItemWriter() {
		ItemWriter<RiverStat> riverStatItemWriter = new RiverItemWriter();

		return riverStatItemWriter;
	}

	private Resource getResource() {
		return context.getResource("file:///Users/glennrenfro/presentations/asug/lightningtalk2/riverbatch/riverdata.csv");
	}
}
