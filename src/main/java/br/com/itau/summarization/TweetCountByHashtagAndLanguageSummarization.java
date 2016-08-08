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
public class TweetCountByHashtagAndLanguageSummarization {

	private static final String QUERY = "select count(id) as count, hashtag, language from tweet where language = 'pt' group by hashtag, language";

	private final Logger LOGGER = LoggerFactory.getLogger(TweetCountByHashtagAndLanguageSummarization.class);
	
	@Autowired
	private SparkConnection sparkConnection;
	
	public void updateTweetCountByHashtagAndLanguage() {
		CassandraSQLContext cassandraSQLContext = sparkConnection.getCassandraSQLContext();
		
		Long startTime = System.currentTimeMillis();
		
        DataFrame tweetCount = cassandraSQLContext.sql(QUERY);
        
        tweetCount.write()
                .format("org.apache.spark.sql.cassandra")
                .option("table", "tweet_count_by_hashtag_and_language")
                .option("keyspace", sparkConnection.getKeyspace())
                .mode(SaveMode.Overwrite).save();
        
        LOGGER.info("Top 5 users by Follower Count took {} ms", (System.currentTimeMillis() - startTime));
	}
}
