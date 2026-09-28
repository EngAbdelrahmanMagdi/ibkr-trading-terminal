package com.project.trading.watchlist.api;

import com.project.trading.watchlist.application.WatchlistService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/watchlist")
public class WatchlistController {

    static final String SYMBOL = "^[A-Z][A-Z0-9.-]{0,11}$";

    public record AddItemRequest(@NotNull @Pattern(regexp = SYMBOL) String symbol) {
    }

    public record ItemResponse(String symbol, int position) {
    }

    public record WatchlistResponse(String id, List<ItemResponse> items) {
    }

    private final WatchlistService watchlist;

    public WatchlistController(WatchlistService watchlist) {
        this.watchlist = watchlist;
    }

    @GetMapping
    public WatchlistResponse get() {
        return toResponse(watchlist.get());
    }

    @PostMapping("/items")
    @ResponseStatus(HttpStatus.CREATED)
    public WatchlistResponse add(@Valid @RequestBody AddItemRequest request) {
        return toResponse(watchlist.add(request.symbol()));
    }

    @DeleteMapping("/items/{symbol}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void remove(@PathVariable("symbol") @Pattern(regexp = SYMBOL) String symbol) {
        watchlist.remove(symbol);
    }

    private static WatchlistResponse toResponse(WatchlistService.Watchlist w) {
        return new WatchlistResponse(w.id().toString(),
                w.items().stream().map(i -> new ItemResponse(i.symbol(), i.position())).toList());
    }
}
