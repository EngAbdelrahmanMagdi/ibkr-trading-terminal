package com.project.trading.news.api;

import com.project.trading.news.application.NewsReader;
import com.project.trading.shared.api.ApiFormat;
import com.project.trading.shared.api.ApiLimits;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class NewsController {
    public record ArticleResponse(String id, List<String> symbols, String headline, String source, String url,
                                  String publishedAt, String rawSummary, Object enrichment) { }
    private final NewsReader news;
    private final ApiLimits limits;
    public NewsController(NewsReader news, ApiLimits limits) { this.news = news; this.limits = limits; }
    @GetMapping("/api/v1/news")
    public ResponseEntity<List<ArticleResponse>> list(@RequestParam String symbol,
                                                     @RequestParam(required = false) Integer limit) {
        var result = news.list(symbol, limits.resolve(limit));
        var response = ResponseEntity.ok().header("X-News-Status", result.status());
        if (result.lastSuccess() != null) response.header("X-News-Last-Refreshed-At", ApiFormat.instant(result.lastSuccess()));
        return response.body(result.articles().stream().map(view -> {
            var a = view.article();
            var e = view.enrichment();
            Object enrichment = e == null ? null : new EnrichmentResponse(e.promptVersion(), e.model(),
                    e.modelVersion(), ApiFormat.instant(e.enrichedAt()), e.insight());
            return new ArticleResponse(a.id().toString(), a.symbols(), a.headline(), a.source(), a.url(),
                    ApiFormat.instant(a.publishedAt()), a.rawSummary(), enrichment);
        }).toList());
    }
    public record EnrichmentResponse(String promptVersion, String model, String modelVersion,
                                     String enrichedAt, java.util.Map<String, Object> insight) { }
}
