package com.project.trading.instrument.api;

import com.project.trading.instrument.application.InstrumentService;
import com.project.trading.instrument.domain.Shortability;
import com.project.trading.shared.api.ApiFormat;
import com.project.trading.shared.api.ApiLimits;
import jakarta.validation.constraints.Size;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/instruments")
public class InstrumentController {

    public record ShortabilityResponse(String status, Long availableQuantity, String borrowFeeRate, String asOf) {
    }

    public record InstrumentResponse(String symbol, String name, String exchange, String currency, String assetType,
                                     ShortabilityResponse shortability) {
    }

    private final InstrumentService instruments;
    private final ApiLimits limits;

    public InstrumentController(InstrumentService instruments, ApiLimits limits) {
        this.instruments = instruments;
        this.limits = limits;
    }

    @GetMapping("/search")
    public List<InstrumentResponse> search(@RequestParam("q") @Size(min = 1, max = 32) String query,
                                           @RequestParam(name = "limit", required = false) Integer limit) {
        return instruments.search(query, limits.resolve(limit)).stream()
                .map(d -> new InstrumentResponse(d.instrument().symbol(), d.instrument().name(),
                        d.instrument().exchange(), d.instrument().currency(), d.instrument().assetType(),
                        shortability(d.shortability())))
                .toList();
    }

    private static ShortabilityResponse shortability(Shortability s) {
        return new ShortabilityResponse(s.status().name(), s.availableQuantity(),
                ApiFormat.decimal(s.borrowFeeRate(), 0), ApiFormat.instant(s.asOf()));
    }
}
