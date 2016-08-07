package br.com.itau.summarization;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import br.com.itau.spark.SparkConnection;

@Component
public class TweetCountByHour {

	@Autowired
	private SparkConnection sparkConnection;
	
	public void updateTweetCountByHour() {
		CassandraSQLContext cassandraSQLContext = sparkConnection.getCassandraSQLContext();
		
        DataFrame tweetCount = cassandraSQLContext
        		.sql("select count(id) as count, hour(postdate) hour, to_date(postdate) date from itau.tweet group by to_date(postdate), hour(postdate)");
        
        tweetCount.write()
                .format("org.apache.spark.sql.cassandra")
                .option("table", "tweet_count_by_hour_and_day")
                .option("keyspace", "itau")
                .mode(SaveMode.Overwrite).save();
	}
}
