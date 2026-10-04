import { Terminal } from "@/features/Terminal";
import { connection } from "next/server";

export default async function Home() {
  await connection();
  return <Terminal />;
}
