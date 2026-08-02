import type { Metadata } from "next";
import DashboardClient from "./dashboard-client";

export const metadata: Metadata = {
  title: "Spotify Logger · Listening overview",
  description: "Spotify listening activity, rankings and trends.",
};

export default function Home() {
  return <DashboardClient />;
}
