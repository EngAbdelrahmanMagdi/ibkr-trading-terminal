"use client";
import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Notifications, type Notice } from "@/lib/notifications";
import { time } from "@/lib/format";
import { Icon } from "@/components/Icon";
import styles from "./terminal.module.css";

export const NotificationContext = createContext<Notifications | null>(null);
export function useNotifications() {
  const store = useContext(NotificationContext);
  if (!store) throw new Error("Notification scope missing");
  return store;
}
export function NotificationObserver() {
  const store = useNotifications();
  const orders = useQuery({
    queryKey: ["orders"],
    queryFn: api.orders,
    refetchInterval: 60000,
  });
  const executions = useQuery({
    queryKey: ["executions"],
    queryFn: api.executions,
    refetchInterval: 60000,
  });
  useEffect(() => {
    if (
      orders.isSuccess &&
      executions.isSuccess &&
      !orders.isFetching &&
      !executions.isFetching
    )
      store.observe(orders.data, executions.data);
  }, [
    store,
    orders.data,
    executions.data,
    orders.isSuccess,
    executions.isSuccess,
    orders.isFetching,
    executions.isFetching,
  ]);
  return null;
}
export function NotificationControl({ ticketOpen }: { ticketOpen: boolean }) {
  const store = useNotifications();
  const items = useSyncExternalStore(
    store.subscribe,
    store.snapshot,
    store.snapshot,
  );
  const [open, setOpen] = useState(false);
  const expanded = open && !ticketOpen;
  const trigger = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const root = useRef<HTMLDivElement>(null);
  const unread = items.filter((item) => item.unread).length;
  function close() {
    setOpen(false);
    trigger.current?.focus();
  }
  useEffect(() => {
    if (!expanded) return;
    panel.current?.focus();
    const outside = (event: PointerEvent) => {
      if (event.target instanceof Node && !root.current?.contains(event.target))
        setOpen(false);
    };
    document.addEventListener("pointerdown", outside);
    return () => document.removeEventListener("pointerdown", outside);
  }, [expanded]);
  return (
    <div ref={root} className={styles.notificationRoot}>
      <button
        ref={trigger}
        className={styles.iconButton}
        aria-label={`Notifications${unread ? `, ${unread} unread` : ""}`}
        aria-expanded={expanded}
        aria-controls="notification-history"
        onClick={() => {
          if (open) close();
          else {
            store.read();
            setOpen(true);
          }
        }}
      >
        <Icon name="bell" />
        {unread > 0 && (
          <span className={styles.notificationCount}>{unread}</span>
        )}
      </button>
      {expanded && (
        <div
          id="notification-history"
          ref={panel}
          tabIndex={-1}
          role="region"
          aria-label="Notification history"
          className={styles.notificationPanel}
          onKeyDown={(event) => {
            if (event.key === "Escape") close();
          }}
        >
          <div className={styles.notificationHeading}>
            <strong>SESSION ACTIVITY</strong>
            <button
              className={styles.iconButton}
              aria-label="Close notifications"
              onClick={close}
            >
              <Icon name="close" />
            </button>
          </div>
          <p className={styles.notificationDisclaimer}>
            Current session · not a complete trading audit
          </p>
          {items.length === 0 ? (
            <p className={styles.notificationEmpty}>
              No new activity this session.
            </p>
          ) : (
            <ol>
              {[...items].reverse().map((item) => (
                <li key={item.id} className={styles[`notice${item.tone}`]}>
                  <div>
                    <strong>{item.text}</strong>
                    <span>{item.detail}</span>
                    <time dateTime={item.at}>{time(item.at)}</time>
                  </div>
                  <button
                    className={styles.iconButton}
                    aria-label={`Dismiss ${item.text}`}
                    onClick={() => store.dismiss(item.id)}
                  >
                    <Icon name="close" width={13} height={13} />
                  </button>
                </li>
              ))}
            </ol>
          )}
        </div>
      )}
      <div
        className={styles.notificationAnnouncer}
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        {items.at(-1)?.text}
      </div>
      {!expanded && !ticketOpen && (
        <div
          className={styles.notificationToasts}
          aria-label="Recent notifications"
        >
          {items
            .filter((item) => item.toast)
            .map((item) => (
              <Toast key={item.id} item={item} store={store} />
            ))}
        </div>
      )}
    </div>
  );
}
function Toast({ item, store }: { item: Notice; store: Notifications }) {
  const remaining = useRef(6000);
  const [paused, setPaused] = useState(false);
  useEffect(() => {
    if (paused) return;
    const started = performance.now();
    const timer = setTimeout(() => store.hideToast(item.id), remaining.current);
    return () => {
      clearTimeout(timer);
      remaining.current = Math.max(
        0,
        remaining.current - (performance.now() - started),
      );
    };
  }, [paused, item.id, store]);
  return (
    <div
      className={`${styles.notificationToast} ${styles[`notice${item.tone}`]}`}
      onMouseEnter={() => setPaused(true)}
      onMouseLeave={() => setPaused(false)}
      onFocus={() => setPaused(true)}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget))
          setPaused(false);
      }}
    >
      <div>
        <strong>{item.text}</strong>
        <span>{item.detail}</span>
      </div>
      <button
        className={styles.iconButton}
        aria-label={`Dismiss toast: ${item.text}`}
        onClick={() => store.hideToast(item.id)}
      >
        <Icon name="close" width={13} height={13} />
      </button>
    </div>
  );
}
