package br.com.itau.summarization;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/summarizarion")
public class SummarizationController {

	@Autowired
	private TopUsersByFollowerCountSummarization topUsersByFollowerCountSummarization;
	
	@Autowired
	private TweetCountByHashtagAndLanguageSummarization tweetCountByHashtagAndLanguageSummarization;
	
	@Autowired
	private TweetCountByHourSummarization tweetCountByHourSummarization;

	@RequestMapping(method = RequestMethod.GET, value = "/top-users")
	public void topUsers() {
		topUsersByFollowerCountSummarization.updateTopUsers();
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/tweet-count-by-hashtag-and-language")
	public void tweetCountByHashtagAndLanguage() {
		tweetCountByHashtagAndLanguageSummarization.updateTweetCountByHashtagAndLanguage();
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/tweet-count-by-hour")
	public void tweetCountByHour() {
		tweetCountByHourSummarization.updateTweetCountByHour();
	}
}
