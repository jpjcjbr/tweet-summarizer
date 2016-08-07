package br.com.itau.summarization;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class SummarizationController {

	@Autowired
	private TopUsersByFollowerCount topUsersByFollowerCount;
	
	@Autowired
	private TweetCountByHashtagAndLanguage tweetCountByHashtagAndLanguage;
	
	@Autowired
	private TweetCountByHour tweetCountByHour;

	@RequestMapping(method = RequestMethod.GET, value = "/top-users")
	public void topUsers() {
		topUsersByFollowerCount.updateTopUsers();
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/tweet-count-by-hashtag-and-language")
	public void tweetCountByHashtagAndLanguage() {
		tweetCountByHashtagAndLanguage.updateTweetCountByHashtagAndLanguage();
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/tweet-count-by-hour")
	public void tweetCountByHour() {
		tweetCountByHour.updateTweetCountByHour();
	}
}
