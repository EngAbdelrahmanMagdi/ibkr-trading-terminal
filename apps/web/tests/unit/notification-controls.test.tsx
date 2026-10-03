import { act, fireEvent, render, screen } from "@testing-library/react";
import {
  NotificationContext,
  NotificationControl,
} from "@/features/Notifications";
import { Notifications } from "@/lib/notifications";

test("notification history opens by keyboard, dismisses and restores focus", () => {
  const store = new Notifications();
  store.failure("key", "Order request failed", "Safe failure detail");
  render(
    <NotificationContext.Provider value={store}>
      <NotificationControl ticketOpen={false} />
    </NotificationContext.Provider>,
  );
  const trigger = screen.getByRole("button", {
    name: "Notifications, 1 unread",
  });
  trigger.focus();
  fireEvent.click(trigger);
  const history = screen.getByRole("region", { name: "Notification history" });
  expect(history).toHaveFocus();
  expect(
    screen.queryByLabelText("Recent notifications"),
  ).not.toBeInTheDocument();
  fireEvent.click(
    screen.getByRole("button", { name: "Dismiss Order request failed" }),
  );
  expect(screen.getByText("No new activity this session.")).toBeVisible();
  fireEvent.keyDown(history, { key: "Escape" });
  expect(trigger).toHaveFocus();
  expect(trigger).toHaveAttribute("aria-expanded", "false");
});
test("ticket drawer suppresses overlay and notification text is safely rendered", () => {
  const store = new Notifications();
  store.failure("key", "<script>unsafe</script>", "request failed");
  const view = render(
    <NotificationContext.Provider value={store}>
      <NotificationControl ticketOpen />
    </NotificationContext.Provider>,
  );
  expect(
    screen.queryByLabelText("Recent notifications"),
  ).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: /Notifications/ }));
  expect(
    screen.queryByRole("region", { name: "Notification history" }),
  ).not.toBeInTheDocument();
  view.rerender(
    <NotificationContext.Provider value={store}>
      <NotificationControl ticketOpen={false} />
    </NotificationContext.Provider>,
  );
  expect(screen.getAllByText("<script>unsafe</script>").length).toBeGreaterThan(
    0,
  );
  expect(document.querySelector("script")).toBeNull();
});

test("toast dismissal pauses on hover and keyboard focus", () => {
  jest.useFakeTimers();
  const store = new Notifications();
  store.failure("key", "Order request failed", "Safe detail");
  render(
    <NotificationContext.Provider value={store}>
      <NotificationControl ticketOpen={false} />
    </NotificationContext.Provider>,
  );
  const toasts = screen.getByLabelText("Recent notifications");
  const toast = toasts.firstElementChild!;
  fireEvent.mouseEnter(toast);
  act(() => jest.advanceTimersByTime(7000));
  expect(toasts).toHaveTextContent("Order request failed");
  fireEvent.mouseLeave(toast);
  act(() => jest.advanceTimersByTime(3000));
  fireEvent.focus(
    screen.getByRole("button", { name: "Dismiss toast: Order request failed" }),
  );
  act(() => jest.advanceTimersByTime(7000));
  expect(toasts).toHaveTextContent("Order request failed");
  fireEvent.blur(
    screen.getByRole("button", { name: "Dismiss toast: Order request failed" }),
  );
  act(() => jest.advanceTimersByTime(6000));
  expect(toasts).not.toHaveTextContent("Order request failed");
  expect(store.snapshot()).toHaveLength(1);
  jest.useRealTimers();
});
