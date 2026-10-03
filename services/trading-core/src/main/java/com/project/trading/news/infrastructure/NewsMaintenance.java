package com.project.trading.news.infrastructure;

import com.project.trading.news.application.NewsService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class NewsMaintenance {
    private final NewsService news;
    public NewsMaintenance(NewsService news) { this.news = news; }
    @Scheduled(fixedDelayString = "${NEWS_CLEANUP_INTERVAL:1h}", initialDelayString = "${NEWS_CLEANUP_INTERVAL:1h}")
    public void cleanup() {
        news.cleanup();
    }
}
