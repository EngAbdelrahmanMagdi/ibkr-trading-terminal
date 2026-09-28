package com.project.trading.broker.infrastructure.ibkr;

import com.project.trading.instrument.domain.Instrument;
import com.project.trading.instrument.domain.InstrumentCatalogPort;
import com.project.trading.shared.domain.Decimals;
import com.project.trading.shared.domain.DomainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Resolves a symbol to exactly one US stock listed in USD:
 * <ol>
 *   <li>GET /trsrv/stocks?symbols= lists the candidate US stock contracts (asset class STK, isUS);</li>
 *   <li>GET /trsrv/secdef?conids= confirms asset class, USD currency, the US listing and the ticker;</li>
 *   <li>POST /iserver/contract/rules gives the price increment ({@code incrementDigits}, else {@code increment}).</li>
 * </ol>
 * No match is "not found"; more than one match is ambiguous and also refused. A broker failure is reported as
 * broker unavailable, never as an unknown symbol. Resolved instruments are persisted by the caller, so each symbol
 * is looked up once. Checked 2026-09-28 against https://www.interactivebrokers.com/docs/web-api/ (Security Stocks by
 * Symbol, Search SecDef information by conid, Search Contract Rules).
 */
final class IbkrInstrumentCatalog implements InstrumentCatalogPort {

    private static final Logger log = LoggerFactory.getLogger(IbkrInstrumentCatalog.class);

    private static final Pattern SYMBOL = Pattern.compile("^[A-Z][A-Z0-9.-]{0,11}$");
    private static final int MAX_CANDIDATES = 50;

    private record Candidate(long conid, String exchange) {
    }

    private final IbkrHttp http;

    IbkrInstrumentCatalog(IbkrHttp http) {
        this.http = http;
    }

    @Override
    public Optional<Instrument> resolve(String symbol) {
        if (symbol == null || !SYMBOL.matcher(symbol).matches()) {
            return Optional.empty();
        }
        try {
            Map<Long, Candidate> candidates = candidates(symbol);
            if (candidates.isEmpty()) {
                return Optional.empty();
            }
            List<Instrument> matches = verify(symbol, candidates);
            if (matches.size() != 1) {
                if (matches.size() > 1) {
                    log.warn("IBKR contract lookup for {} is ambiguous ({} listings); symbol refused", symbol, matches.size());
                }
                return Optional.empty();
            }
            Instrument match = matches.getFirst();
            Integer scale = priceScale(match.conid());
            if (scale == null) {
                log.warn("IBKR contract rules for {} have no usable price increment; symbol not resolved", symbol);
                throw DomainException.brokerUnavailable("the broker did not provide the price increment for " + symbol);
            }
            return Optional.of(new Instrument(match.symbol(), match.conid(), match.name(), match.exchange(),
                    match.currency(), match.assetType(), scale));
        } catch (IbkrHttp.CallException e) {
            throw DomainException.brokerUnavailable("the broker instrument catalog is not available");
        }
    }

    /** Exact symbol lookup: the catalog has no free-text search in this version. */
    @Override
    public List<Instrument> search(String query, int limit) {
        if (limit < 1) {
            return List.of();
        }
        return resolve(query).map(List::of).orElse(List.of());
    }

    private Map<Long, Candidate> candidates(String symbol) throws IbkrHttp.CallException {
        IbkrHttp.Response response = http.get(IbkrEndpoint.STOCKS, "/trsrv/stocks?symbols=" + symbol);
        requireOk(response);
        Map<Long, Candidate> out = new LinkedHashMap<>();
        for (JsonNode entry : response.body().path(symbol)) {
            if (!"STK".equals(IbkrJson.id(entry.get("assetClass")))) {
                continue;
            }
            for (JsonNode contract : entry.path("contracts")) {
                Long conid = IbkrJson.whole(contract.get("conid"));
                if (conid != null && conid > 0 && IbkrJson.isTrue(contract.get("isUS")) && out.size() < MAX_CANDIDATES) {
                    out.putIfAbsent(conid, new Candidate(conid, IbkrJson.id(contract.get("exchange"))));
                }
            }
        }
        return out;
    }

    private List<Instrument> verify(String symbol, Map<Long, Candidate> candidates) throws IbkrHttp.CallException {
        StringBuilder ids = new StringBuilder();
        for (Long conid : candidates.keySet()) {
            ids.append(ids.isEmpty() ? "" : ",").append(conid);
        }
        IbkrHttp.Response response = http.get(IbkrEndpoint.SECDEF, "/trsrv/secdef?conids=" + ids);
        requireOk(response);
        List<Instrument> matches = new ArrayList<>();
        for (JsonNode s : response.body().path("secdef")) {
            Long conid = IbkrJson.whole(s.get("conid"));
            String ticker = IbkrJson.id(s.get("ticker"));
            if (conid == null || !candidates.containsKey(conid) || !"STK".equals(IbkrJson.id(s.get("assetClass")))
                    || !"USD".equals(IbkrJson.id(s.get("currency"))) || !IbkrJson.isTrue(s.get("isUS"))
                    || (ticker != null && !ticker.isEmpty() && !ticker.equalsIgnoreCase(symbol))) {
                continue;
            }
            String exchange = IbkrJson.id(s.get("listingExchange"));
            if (exchange == null || exchange.isBlank()) {
                exchange = candidates.get(conid).exchange();
            }
            if (exchange == null || exchange.isBlank() || exchange.length() > 32) {
                log.warn("IBKR contract {} for {} has no listing exchange; skipped", conid, symbol);
                continue;
            }
            String name = IbkrJson.sanitize(IbkrJson.id(s.get("name")), 200);
            matches.add(new Instrument(symbol, conid, name, exchange, "USD", "STK", 0));
        }
        return matches;
    }

    /** Decimal places of the minimum price increment, at most the price scale; null when not provided. */
    private Integer priceScale(long conid) throws IbkrHttp.CallException {
        ObjectNode body = IbkrJson.MAPPER.createObjectNode().put("conid", conid).put("isBuy", true);
        IbkrHttp.Response response = http.post(IbkrEndpoint.CONTRACT_RULES, "/iserver/contract/rules", body);
        requireOk(response);
        Long digits = IbkrJson.whole(response.body().get("incrementDigits"));
        if (digits != null) {
            return digits <= Decimals.PRICE_SCALE ? digits.intValue() : null;
        }
        BigDecimal increment = IbkrJson.decimal(response.body().get("increment"));
        if (increment != null && increment.signum() > 0) {
            int scale = Decimals.significantScale(increment);
            return scale <= Decimals.PRICE_SCALE ? scale : null;
        }
        return null;
    }

    private static void requireOk(IbkrHttp.Response response) throws IbkrHttp.CallException {
        if (!response.ok()) {
            throw new IbkrHttp.CallException(IbkrHttp.CallException.Kind.MAYBE_SENT, "HTTP " + response.status());
        }
    }
}
