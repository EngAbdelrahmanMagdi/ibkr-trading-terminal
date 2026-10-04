package com.project.trading.shared.api;

import com.project.trading.shared.config.AppProperties;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import jakarta.servlet.FilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import static org.mockito.Mockito.verifyNoInteractions;

class RequestAdmissionFilterTest {
    @Test void disallowedOriginCannotReachMutationAndForwardedHeadersDoNotSetIdentity() throws Exception {
        AppProperties properties = mock(AppProperties.class);
        AppProperties.Http http = mock(AppProperties.Http.class);
        when(properties.http()).thenReturn(http);
        when(http.corsAllowedOrigins()).thenReturn(List.of("http://localhost:3000"));
        var filter = new RequestAdmissionFilter(Clock.systemUTC(), mock(ProblemWriter.class), properties, 600, 60, 60, 120);
        var request = new MockHttpServletRequest("POST", "/api/v1/orders");
        request.addHeader("Origin", "https://localhost:3000");
        request.addHeader("X-Forwarded-For", "new-peer");
        FilterChain chain = mock(FilterChain.class);
        filter.doFilter(request, new MockHttpServletResponse(), chain);
        verifyNoInteractions(chain);
    }
    @Test void commandBudgetAndCapacityAreIndependentFromSubmissionRate() {
        AppProperties properties = mock(AppProperties.class);
        AppProperties.Http http = mock(AppProperties.Http.class);
        when(properties.http()).thenReturn(http);
        when(http.corsAllowedOrigins()).thenReturn(List.of("http://localhost:3000"));
        var filter = new RequestAdmissionFilter(Clock.systemUTC(), mock(ProblemWriter.class), properties, 600, 60, 60, 120);
        assertThat(filter.admit("peer", "submit", 60, 1, 0)).isEqualTo(200);
        assertThat(filter.admit("peer", "submit", 60, 1, 0)).isEqualTo(429);
        assertThat(filter.admit("peer", "command", 120, 1, 0)).isEqualTo(200);
        for (int i = 0; i < 4095; i++) filter.admit("peer" + i, "read", 600, 100, 0);
        assertThat(filter.admit("extra", "read", 600, 100, 0)).isEqualTo(503);
        assertThat(filter.admit("extra", "read", 600, 100, 600_000)).isEqualTo(200);
    }
}
