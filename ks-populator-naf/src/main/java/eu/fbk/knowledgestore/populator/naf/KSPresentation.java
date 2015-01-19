package eu.fbk.knowledgestore.populator.naf;

import java.util.Collection;
import java.util.Hashtable;

import eu.fbk.knowledgestore.data.Record;

public class KSPresentation {
	private String news;
    private String naf_file_path;
	private Hashtable<String, Record> mentions;
	private Record naf;
	private Record newsResource;
	private statistics stats;
    
    public statistics getStats() {
        return stats;
    }

    
    public void setStats(statistics stats) {
        this.stats = stats;
    }


	public Record getNaf() {
		return naf;
	}
	public void setNaf(Record naf) {
		this.naf = naf;
	}
	public Record getNewsResource() {
		return newsResource;
	}
	public void setNewsResource(Record newsResource) {
		this.newsResource = newsResource;
	}
	public String getNews() {
		return news;
	}
	public void setNews(String news) {
		this.news = news;
	}
	public String getNaf_file_path() {
		return naf_file_path;
	}
	public void setNaf_file_path(String naf_file_path) {
		this.naf_file_path = naf_file_path;
	}
	public Hashtable<String, Record> getMentions() {
		return mentions;
	}
	public void setMentions(Hashtable<String, Record> mentionListHash) {
		this.mentions = mentionListHash;
	}
}
