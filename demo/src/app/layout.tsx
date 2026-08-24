import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Sync Bench",
  description: "PostgreSQL to MongoDB customer replication workspace.",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en">
      <body>
        {/* THESIS: A PCB diagnostic bench makes migration architecture legible, replacing a novelty event feed with a control plane that separates observation from proof. OWN-WORLD: cobalt solder-mask ground, copper traces, ivory record ledgers, cyan observation current. STORY: a solutions architect sees where authority, transaction integrity, projection, and parity gates sit before evaluating replica convergence. FIRST VIEWPORT: a five-stage replication rail frames source and replica ledgers around a central control plane. FORM: PCB diagnostic bench, evolved for architecture review. FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, and DESIGN.md. */}
        {children}
      </body>
    </html>
  );
}
