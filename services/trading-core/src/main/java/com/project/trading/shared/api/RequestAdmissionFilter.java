package com.project.trading.shared.api;

import com.project.trading.shared.config.AppProperties;
import com.project.trading.shared.domain.ErrorCategory;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Bounded peer admission; independent command budgets and no forwarded-header trust. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 2)
public class RequestAdmissionFilter extends OncePerRequestFilter {
    private static final long IDLE_MILLIS = 600_000;
    private static final int CAPACITY = 4096;
    private static final class Bucket { double tokens; long at; Bucket(int burst, long now) { tokens = burst; at = now; } }
    private static final class Peer { long touched; final Map<String, Bucket> policies = new HashMap<>(); }
    private final Map<String, Peer> peers = new HashMap<>();
    private final Clock clock;
    private final ProblemWriter problems;
    private final List<String> origins;
    private final int readRate, newsRate, submitRate, commandRate;

    public RequestAdmissionFilter(Clock clock, ProblemWriter problems, AppProperties properties,
            @Value("${http.admission.reads:600}") int readRate,
            @Value("${http.admission.news:60}") int newsRate,
            @Value("${http.admission.submissions:60}") int submitRate,
            @Value("${http.admission.commands:120}") int commandRate) {
        if (readRate <= 0 || newsRate <= 0 || submitRate <= 0 || commandRate <= 0) throw new IllegalArgumentException("Invalid admission rates");
        this.clock = clock; this.problems = problems; this.origins = properties.http().corsAllowedOrigins();
        this.readRate = readRate; this.newsRate = newsRate; this.submitRate = submitRate; this.commandRate = commandRate;
    }

    @Override protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("Referrer-Policy", "no-referrer");
        String origin = request.getHeader("Origin");
        if (origin != null && !origins.contains(origin)) {
            problems.write(response, ProblemWriter.of(ErrorCategory.VALIDATION, 403, "Origin not allowed", "browser origin is not allowed", List.of()));
            return;
        }
        if ("OPTIONS".equals(request.getMethod())) { chain.doFilter(request, response); return; }
        String path = request.getRequestURI();
        boolean read = "GET".equals(request.getMethod());
        String policy = read ? path.equals("/api/v1/news") ? "news" : "read"
                : path.equals("/api/v1/orders") ? "submit" : "command";
        int rate = switch (policy) { case "news" -> newsRate; case "submit" -> submitRate; case "command" -> commandRate; default -> readRate; };
        int burst = switch (policy) { case "read" -> 100; case "command" -> 20; default -> 10; };
        int result = admit(request.getRemoteAddr(), policy, rate, burst, clock.millis());
        if (result != 200) {
            if (origin != null) { response.setHeader("Access-Control-Allow-Origin", origin); response.addHeader("Vary", "Origin"); }
            if (result == 429) response.setHeader("Retry-After", "2");
            problems.write(response, ProblemWriter.of(result == 429 ? ErrorCategory.RATE_LIMITED : ErrorCategory.SERVICE_UNAVAILABLE,
                    result, result == 429 ? "Too many requests" : "Service unavailable", "request admission is temporarily unavailable", List.of()));
            return;
        }
        chain.doFilter(request, response);
    }

    synchronized int admit(String address, String policy, int rate, int burst, long now) {
        Peer peer = peers.get(address);
        if (peer == null) {
            if (peers.size() >= CAPACITY) peers.entrySet().removeIf(e -> now - e.getValue().touched >= IDLE_MILLIS);
            if (peers.size() >= CAPACITY) return 503;
            peer = new Peer(); peers.put(address, peer);
        }
        peer.touched = now;
        Bucket bucket = peer.policies.computeIfAbsent(policy, ignored -> new Bucket(burst, now));
        bucket.tokens = Math.min(burst, bucket.tokens + Math.max(0, now - bucket.at) * rate / 60_000.0);
        bucket.at = now;
        if (bucket.tokens < 1) return 429;
        bucket.tokens--;
        return 200;
    }
}
