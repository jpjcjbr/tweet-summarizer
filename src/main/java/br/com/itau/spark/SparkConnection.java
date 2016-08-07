package br.com.itau.spark;

import javax.annotation.PostConstruct;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.stereotype.Component;

@Component
public class SparkConnection {

	private CassandraSQLContext cassandraSQLContext;

	@PostConstruct
	public void init() {
		final SparkConf conf = new SparkConf(true);
        conf.setMaster("local[*]");
        conf.setAppName("tweets-itau");

        conf.set(
                "spark.cassandra.connection.host",
                "192.168.99.100");

        SparkContext sc = new SparkContext(conf);

        cassandraSQLContext = new CassandraSQLContext(sc);
	}
	
	public CassandraSQLContext getCassandraSQLContext() {
		return cassandraSQLContext;
	}
}
