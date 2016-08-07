package br.com.itau.summarization;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import br.com.itau.spark.SparkConnection;

@Component
public class TweetCountByHashtagAndLanguage {

	@Autowired
	private SparkConnection sparkConnection;
	
	public void updateTweetCountByHashtagAndLanguage() {
		CassandraSQLContext cassandraSQLContext = sparkConnection.getCassandraSQLContext();
		
        DataFrame tweetCount = cassandraSQLContext
                .sql("select count(id) as count, hashtag, language from itau.tweet where language = 'pt' group by hashtag, language");
        
        tweetCount.write()
                .format("org.apache.spark.sql.cassandra")
                .option("table", "tweet_count_by_hashtag_and_language")
                .option("keyspace", "itau")
                .mode(SaveMode.Overwrite).save();
	}
}
