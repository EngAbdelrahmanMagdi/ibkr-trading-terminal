"use client";

export default function ErrorBoundary({
  reset,
}: {
  error: Error;
  reset: () => void;
}) {
  return (
    <main
      style={{
        minHeight: "100vh",
        display: "grid",
        placeContent: "center",
        background: "#0b1118",
        color: "#ebf0f2",
        fontFamily: "sans-serif",
        textAlign: "center",
        gap: 12,
      }}
    >
      <h1 style={{ fontSize: 20, margin: 0 }}>
        The workspace could not render
      </h1>
      <p style={{ color: "#9aadb9", margin: 0 }}>
        A service may have returned unexpected data. Check the connection and
        try again.
      </p>
      <button
        style={{
          justifySelf: "center",
          padding: "9px 16px",
          background: "#245063",
          color: "white",
          border: 0,
          borderRadius: 3,
        }}
        onClick={reset}
      >
        Retry workspace
      </button>
    </main>
  );
}
