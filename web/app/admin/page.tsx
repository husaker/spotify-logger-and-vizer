import type { Metadata } from "next";
import AdminClient from "./admin-client";

export const metadata: Metadata = {
  title: "Admin · Spotify Logger",
  description: "Protected Spotify Logger operations and notification setup.",
};

export default function AdminPage() {
  return <AdminClient />;
}

