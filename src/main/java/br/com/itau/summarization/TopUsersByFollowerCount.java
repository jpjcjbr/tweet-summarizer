package br.com.itau.summarization;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import br.com.itau.spark.SparkConnection;

@Component
public class TopUsersByFollowerCount {

	@Autowired
	private SparkConnection sparkConnection;
	
	public void updateTopUsers() {
		CassandraSQLContext cassandraSQLContext = sparkConnection.getCassandraSQLContext();
		
        DataFrame topUsersByFollowerCount = cassandraSQLContext
                .sql("select distinct(username), followerscount from itau.tweet order by followerscount desc limit 5");

        topUsersByFollowerCount.write()
                .format("org.apache.spark.sql.cassandra")
                .option("table", "top_users_by_follower_count")
                .option("keyspace", "itau")
                .mode(SaveMode.Overwrite).save();
	}
}
