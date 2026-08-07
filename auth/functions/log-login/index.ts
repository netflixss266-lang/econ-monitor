// ============================================================
// Edge Function: log-login
// บันทึกการเข้าใช้ลงตาราง login_events พร้อม IP จริงของผู้ใช้
//
// ทำไมต้องมีตัวนี้ แทนที่จะให้หน้าเว็บเขียนลงฐานข้อมูลตรงๆ:
//   1. IP ที่หน้าเว็บส่งมาเองปลอมได้ทันที ต้องอ่านจาก header ที่ edge ใส่มาให้
//   2. ตาราง login_events ปิดสิทธิ์เขียนไว้ ให้เขียนได้เฉพาะ service_role
//      ซึ่งคีย์ตัวนั้นต้องอยู่ฝั่งเซิร์ฟเวอร์เท่านั้น ห้ามหลุดไปหน้าเว็บเด็ดขาด
//
// deploy:  supabase functions deploy log-login
// ============================================================

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

type Event =
  | "success" | "fail" | "lockout" | "logout" | "reset_request" | "reset_done";

const ALLOWED: Event[] = [
  "success", "fail", "lockout", "logout", "reset_request", "reset_done",
];

/** IP จริงของผู้ใช้
 *
 *  ลำดับความน่าเชื่อถือ: cf-connecting-ip (Cloudflare เขียนทับให้เสมอ ปลอมไม่ได้)
 *  → x-real-ip → ตัวซ้ายสุดของ x-forwarded-for
 *
 *  ข้อควรรู้: x-forwarded-for ตัวซ้ายสุดจะเชื่อถือได้ก็ต่อเมื่อ proxy ด่านนอกสุด
 *  "เขียนทับ" ไม่ใช่ "ต่อท้าย" ค่าที่ client ส่งมา — ของ Supabase เขียนทับให้
 *  แต่ถ้าย้ายไปวางหลัง proxy ตัวอื่นต้องมาตรวจข้อนี้ใหม่
 */
function clientIp(h: Headers): string | null {
  const cf = h.get("cf-connecting-ip");
  if (cf) return cf.trim();
  const real = h.get("x-real-ip");
  if (real) return real.trim();
  const fwd = h.get("x-forwarded-for");
  if (fwd) return fwd.split(",")[0].trim() || null;
  return null;
}

/** ตัดส่วนท้ายของ IP ทิ้ง ไว้ใช้ทำสถิติโดยไม่ชี้ตัวบุคคล
 *  IPv4 เหลือ /24 · IPv6 เหลือ /48 ตามแนวปฏิบัติที่ใช้กันทั่วไป */
function ipPrefix(ip: string | null): string | null {
  if (!ip) return null;
  if (ip.includes(":")) {
    const parts = ip.split(":");
    return parts.slice(0, 3).join(":") + "::";
  }
  const parts = ip.split(".");
  if (parts.length !== 4) return null;
  return `${parts[0]}.${parts[1]}.${parts[2]}.0`;
}

const CORS = {
  "Access-Control-Allow-Origin": Deno.env.get("SITE_ORIGIN") ?? "*",
  "Access-Control-Allow-Headers": "authorization, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
  if (req.method !== "POST") {
    return new Response("method not allowed", { status: 405, headers: CORS });
  }

  let body: { event?: string; email?: string };
  try {
    body = await req.json();
  } catch {
    return new Response("bad request", { status: 400, headers: CORS });
  }

  const event = body.event as Event;
  if (!ALLOWED.includes(event)) {
    return new Response("unknown event", { status: 400, headers: CORS });
  }

  const admin = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    { auth: { persistSession: false } },
  );

  // ถ้าเข้าสู่ระบบสำเร็จจะมี token ติดมา เอามาหาว่าเป็นบัญชีไหน
  // ส่วนกรณีที่ล็อกอินไม่ผ่านจะไม่มี token จึงเก็บได้แค่อีเมลที่พยายามใช้
  let userId: string | null = null;
  const authz = req.headers.get("authorization");
  if (authz?.startsWith("Bearer ")) {
    const { data } = await admin.auth.getUser(authz.slice(7));
    userId = data.user?.id ?? null;
  }

  const ip = clientIp(req.headers);
  const { error } = await admin.from("login_events").insert({
    user_id: userId,
    // เก็บอีเมลที่กรอกเฉพาะตอนที่ยังไม่รู้ว่าเป็นใคร จะได้ไม่เก็บซ้ำโดยไม่จำเป็น
    email_tried: userId ? null : (body.email ?? null)?.slice(0, 254),
    event,
    ip,
    ip_prefix: ipPrefix(ip),
    country: req.headers.get("cf-ipcountry") ??
             req.headers.get("x-vercel-ip-country") ?? null,
    user_agent: req.headers.get("user-agent")?.slice(0, 400) ?? null,
  });

  if (error) {
    console.error("insert login_events failed:", error.message);
    return new Response("log failed", { status: 500, headers: CORS });
  }
  // ไม่ต้องคืนอะไรกลับไป — หน้าเว็บไม่ควรได้ข้อมูลอะไรจากการบันทึกครั้งนี้
  return new Response(null, { status: 204, headers: CORS });
});
