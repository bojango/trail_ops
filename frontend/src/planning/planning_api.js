export async function planningListRoutes() {
  const res = await fetch("/planning/routes");
  if (!res.ok) throw new Error("planning/routes failed: " + res.status);
  return await res.json();
}
