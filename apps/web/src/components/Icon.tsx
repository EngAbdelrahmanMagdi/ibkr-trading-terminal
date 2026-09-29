import type { SVGProps } from "react";

type Name =
  | "search"
  | "plus"
  | "close"
  | "chevron"
  | "bars"
  | "bolt"
  | "arrow"
  | "refresh"
  | "menu";

const paths: Record<Name, React.ReactNode> = {
  search: (
    <>
      <circle cx="11" cy="11" r="6.5" />
      <path d="m16 16 4 4" />
    </>
  ),
  plus: <path d="M12 5v14M5 12h14" />,
  close: <path d="M6 6 18 18M18 6 6 18" />,
  chevron: <path d="m7 10 5 5 5-5" />,
  bars: (
    <>
      <path d="M4 18V9m5 9V5m5 13v-7m5 7V3" />
    </>
  ),
  bolt: <path d="m13 2-8 11h6l-1 9 9-12h-6V2Z" />,
  arrow: <path d="M5 12h14m-6-6 6 6-6 6" />,
  refresh: (
    <>
      <path d="M20 11a8 8 0 0 0-14-5L4 8m0-4v4h4" />
      <path d="M4 13a8 8 0 0 0 14 5l2-2m0 4v-4h-4" />
    </>
  ),
  menu: <path d="M4 7h16M4 12h16M4 17h16" />,
};

export function Icon({
  name,
  ...props
}: SVGProps<SVGSVGElement> & { name: Name }) {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    >
      {paths[name]}
    </svg>
  );
}
