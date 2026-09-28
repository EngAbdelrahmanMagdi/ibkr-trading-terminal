package com.project.trading.execution.api;

import com.project.trading.execution.application.ExecutionLedger;
import com.project.trading.execution.domain.Execution;
import com.project.trading.shared.api.ApiFormat;
import com.project.trading.shared.api.ApiLimits;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionController {

    public record ExecutionResponse(String id, String orderId, String symbol, String side, String quantity,
                                    String price, String commission, String currency, String executedAt) {
    }

    private final ExecutionLedger executions;
    private final ApiLimits limits;

    public ExecutionController(ExecutionLedger executions, ApiLimits limits) {
        this.executions = executions;
        this.limits = limits;
    }

    @GetMapping
    public List<ExecutionResponse> list(@RequestParam(name = "orderId", required = false) UUID orderId,
                                        @RequestParam(name = "limit", required = false) Integer limit) {
        return executions.list(orderId, limits.resolve(limit)).stream().map(ExecutionController::toResponse).toList();
    }

    private static ExecutionResponse toResponse(Execution e) {
        return new ExecutionResponse(e.id().toString(), e.orderId().toString(), e.symbol(), e.side().name(),
                ApiFormat.decimal(e.quantity().value(), 0), ApiFormat.decimal(e.price().value(), 2),
                ApiFormat.decimal(e.commission(), 2), e.currency(), ApiFormat.instant(e.executedAt()));
    }
}
