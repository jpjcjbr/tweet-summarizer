package br.com.itau.spark;

import javax.annotation.PostConstruct;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SparkConnection {

	private CassandraSQLContext cassandraSQLContext;
	
	@Value("${spring.data.cassandra.contact-points}")
	private String cassandraContactPoints;
	
	@Value("${spring.data.cassandra.keyspace-name}")
	private String keyspace;

	@PostConstruct
	public void init() {
		final SparkConf conf = new SparkConf(true);
        conf.setMaster("local[*]");
        conf.setAppName("tweets-itau");

        conf.set("spark.cassandra.connection.host", cassandraContactPoints);

        SparkContext sc = new SparkContext(conf);

        cassandraSQLContext = new CassandraSQLContext(sc);
        cassandraSQLContext.setKeyspace(keyspace);
	}
	
	public CassandraSQLContext getCassandraSQLContext() {
		return cassandraSQLContext;
	}
	
	public String getKeyspace() {
		return keyspace;
	}
}
