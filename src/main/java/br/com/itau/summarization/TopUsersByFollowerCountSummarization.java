package br.com.itau.summarization;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import br.com.itau.spark.SparkConnection;

@Component
public class TopUsersByFollowerCountSummarization {

	private final Logger LOGGER = LoggerFactory.getLogger(TopUsersByFollowerCountSummarization.class);
	
	private static final String QUERY = "select distinct(username), followerscount from tweet order by followerscount desc limit 5";
	
	@Autowired
	private SparkConnection sparkConnection;
	
	public void updateTopUsers() {
		CassandraSQLContext cassandraSQLContext = sparkConnection.getCassandraSQLContext();
		
		Long startTime = System.currentTimeMillis();
		
        DataFrame topUsersByFollowerCount = cassandraSQLContext.sql(QUERY);

        topUsersByFollowerCount.write()
                .format("org.apache.spark.sql.cassandra")
                .option("table", "top_users_by_follower_count")
                .option("keyspace", sparkConnection.getKeyspace())
                .mode(SaveMode.Overwrite).save();
        
        LOGGER.info("Top 5 users by Follower Count took {} ms", (System.currentTimeMillis() - startTime));
	}
}
