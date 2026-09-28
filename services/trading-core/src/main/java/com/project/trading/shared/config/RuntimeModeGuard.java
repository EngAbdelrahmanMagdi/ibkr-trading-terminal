package com.project.trading.shared.config;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Deployment rules of the runtime modes. MOCK runs anywhere. IBKR_PAPER can place orders in a broker account
 * and the API has no login, so it requires an explicit trusted-environment flag and a CORS allowlist of
 * loopback or private-network origins only. Violations fail startup (fail closed).
 */
public final class RuntimeModeGuard {

    private static final Pattern IPV4 = Pattern.compile("^\\d{1,3}(\\.\\d{1,3}){3}$");

    private RuntimeModeGuard() {
    }

    /** Throws IllegalStateException listing every violated rule. */
    public static void check(String mode, boolean trustedEnvironment, List<String> corsOrigins) {
        RuntimeMode runtimeMode;
        try {
            runtimeMode = RuntimeMode.valueOf(mode);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unsupported runtime mode '" + mode + "': use MOCK or IBKR_PAPER");
        }
        if (runtimeMode == RuntimeMode.MOCK) {
            return;
        }
        List<String> errors = new ArrayList<>();
        if (!trustedEnvironment) {
            errors.add("APP_TRUSTED_ENVIRONMENT=true is required for IBKR_PAPER; public deployments run MOCK");
        }
        if (corsOrigins.isEmpty()) {
            errors.add("IBKR_PAPER requires an explicit CORS allowlist");
        }
        for (String origin : corsOrigins) {
            if (!isPrivateOrigin(origin)) {
                errors.add("IBKR_PAPER allows only loopback or private-network CORS origins: " + origin);
            }
        }
        if (!errors.isEmpty()) {
            throw new IllegalStateException("IBKR_PAPER startup refused: " + String.join("; ", errors));
        }
    }

    /** localhost, *.localhost, or a loopback or private IPv4 literal (never resolved through DNS). */
    static boolean isPrivateOrigin(String origin) {
        String host;
        try {
            host = URI.create(origin).getHost();
        } catch (IllegalArgumentException e) {
            return false;
        }
        if (host == null) {
            return false;
        }
        host = host.toLowerCase(Locale.ROOT);
        if (host.equals("localhost") || host.endsWith(".localhost")) {
            return true;
        }
        if (!IPV4.matcher(host).matches()) {
            return false;
        }
        try {
            InetAddress address = InetAddress.getByName(host);
            return address.isLoopbackAddress() || address.isSiteLocalAddress();
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
