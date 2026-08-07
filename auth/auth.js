/* ============================================================
   The Tribune — ตัวกลางระหว่างหน้าเว็บกับระบบบัญชี

   มีสองร่าง ใช้หน้าตาเดียวกัน:
     mockAuth      ทำงานในเบราว์เซอร์ล้วน ไว้ลองหน้าตาและขั้นตอนก่อนต่อของจริง
     supabaseAuth  ของจริง — เปิดใช้ได้ทันทีที่ใส่ค่าใน CONFIG ข้างล่าง

   เปลี่ยนร่างที่บรรทัดสุดท้ายบรรทัดเดียว ส่วนหน้าเว็บไม่ต้องแก้อะไรเลย
   ============================================================ */
(() => {
  "use strict";

  const CONFIG = {
    // ได้จาก Supabase → Project Settings → API
    // anon key ออกแบบมาให้เปิดเผยได้ ใส่ในโค้ดหน้าเว็บได้ตามปกติ
    // (ของจริงที่ห้ามหลุดคือ service_role key ซึ่งอยู่ใน Edge Function เท่านั้น)
    url: "",
    anonKey: "",
    logFn: "log-login",
  };

  const MIN_PASSWORD = 10;
  // ตอบข้อความเดียวกันทุกกรณีที่เข้าไม่ได้ เพื่อไม่ให้ไล่เดาว่าอีเมลไหนสมัครไว้แล้ว
  const BAD_CREDS = "อีเมลหรือรหัสผ่านไม่ถูกต้อง";

  const wait = (ms) => new Promise((r) => setTimeout(r, ms));
  const okEmail = (s) => /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(s || "");

  function check(email, password) {
    if (!okEmail(email)) throw new Error("รูปแบบอีเมลไม่ถูกต้อง");
    if ((password || "").length < MIN_PASSWORD) {
      throw new Error(`รหัสผ่านต้องยาวอย่างน้อย ${MIN_PASSWORD} ตัว`);
    }
  }

  /* ── ร่างทดลอง — ข้อมูลอยู่ในเบราว์เซอร์เครื่องนี้เท่านั้น ─────────
     ไม่ใช่ระบบความปลอดภัยจริง และไม่ได้ตั้งใจให้เป็น มีไว้เพื่อ
     เคาะหน้าตากับลำดับขั้นตอนก่อนผูกกับ Supabase เท่านั้น
     หน้า account.html จะไม่ยอมเปิดร่างนี้ถ้าไม่ได้รันอยู่บนเครื่องตัวเอง  */
  const mockAuth = (() => {
    const KEY = "tribune.mock.users";
    const SESSION = "tribune.mock.session";
    const EVENTS = "tribune.mock.events";

    const read = (k, d) => {
      try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch { return d; }
    };
    const write = (k, v) => {
      try { localStorage.setItem(k, JSON.stringify(v)); } catch {}
    };

    async function digest(text) {
      const buf = await crypto.subtle.digest(
        "SHA-256", new TextEncoder().encode(text));
      return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
    }

    function log(event, email, userId) {
      const events = read(EVENTS, []);
      events.unshift({
        id: Date.now() + Math.random(),
        user_id: userId ?? null,
        email_tried: userId ? null : email,
        event,
        // ของจริงค่านี้มาจาก header ฝั่งเซิร์ฟเวอร์ ไม่ใช่จากเบราว์เซอร์
        ip: "203.150.11." + (10 + Math.floor(Math.random() * 40)),
        country: "TH",
        user_agent: navigator.userAgent,
        created_at: new Date().toISOString(),
      });
      write(EVENTS, events.slice(0, 200));
    }

    return {
      mode: "mock",
      async signUp(email, password) {
        check(email, password);
        await wait(350);
        const users = read(KEY, {});
        // ตอบเหมือนกันทั้งกรณีที่อีเมลซ้ำและไม่ซ้ำ ของจริงก็ทำแบบนี้
        if (!users[email]) {
          users[email] = { id: crypto.randomUUID(), hash: await digest(password) };
          write(KEY, users);
        }
        return { pending: true };
      },
      async signIn(email, password) {
        check(email, password);
        await wait(350);
        const users = read(KEY, {});
        const u = users[email];
        if (!u || u.hash !== await digest(password)) {
          log("fail", email, null);
          throw new Error(BAD_CREDS);
        }
        write(SESSION, { email, id: u.id, at: Date.now() });
        log("success", email, u.id);
        return { email, id: u.id };
      },
      async signOut() {
        const s = read(SESSION, null);
        if (s) log("logout", s.email, s.id);
        try { localStorage.removeItem(SESSION); } catch {}
      },
      async getSession() {
        return read(SESSION, null);
      },
      async resetRequest(email) {
        await wait(300);
        log("reset_request", email, null);
        return true;      // ตอบสำเร็จเสมอ ไม่บอกว่าอีเมลนี้มีอยู่จริงไหม
      },
      async listEvents() {
        const s = read(SESSION, null);
        if (!s) return [];
        return read(EVENTS, []).filter((e) => e.user_id === s.id || e.email_tried === s.email);
      },
    };
  })();

  /* ── ร่างจริง ─────────────────────────────────────────────
     ต้องโหลด @supabase/supabase-js ในหน้าเว็บก่อน แล้วใส่ค่าใน CONFIG  */
  const supabaseAuth = (() => {
    let client = null;
    const sb = () => {
      if (!client) {
        if (!CONFIG.url || !CONFIG.anonKey) {
          throw new Error("ยังไม่ได้ใส่ค่า Supabase ใน CONFIG");
        }
        client = window.supabase.createClient(CONFIG.url, CONFIG.anonKey);
      }
      return client;
    };

    // ยิงไปที่ Edge Function เพื่อให้ฝั่งเซิร์ฟเวอร์เป็นคนอ่าน IP เอง
    async function logEvent(event, email) {
      try {
        const { data } = await sb().auth.getSession();
        await fetch(`${CONFIG.url}/functions/v1/${CONFIG.logFn}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...(data?.session?.access_token
              ? { authorization: `Bearer ${data.session.access_token}` } : {}),
            apikey: CONFIG.anonKey,
          },
          body: JSON.stringify({ event, email }),
        });
      } catch (e) {
        // บันทึกไม่สำเร็จต้องไม่ทำให้ผู้ใช้เข้าเว็บไม่ได้
        console.warn("log-login failed:", e);
      }
    }

    return {
      mode: "supabase",
      async signUp(email, password) {
        check(email, password);
        const { error } = await sb().auth.signUp({ email, password });
        if (error) throw new Error("สมัครไม่สำเร็จ ลองใหม่อีกครั้ง");
        return { pending: true };      // ต้องไปกดยืนยันในอีเมลก่อน
      },
      async signIn(email, password) {
        check(email, password);
        const { data, error } = await sb().auth
          .signInWithPassword({ email, password });
        if (error) {
          await logEvent("fail", email);
          throw new Error(BAD_CREDS);
        }
        await logEvent("success", email);
        return { email: data.user.email, id: data.user.id };
      },
      async signOut() {
        await logEvent("logout");
        await sb().auth.signOut();
      },
      async getSession() {
        const { data } = await sb().auth.getSession();
        if (!data.session) return null;
        return { email: data.session.user.email, id: data.session.user.id };
      },
      async resetRequest(email) {
        await sb().auth.resetPasswordForEmail(email, {
          redirectTo: location.origin + location.pathname + "?reset=1",
        });
        await logEvent("reset_request", email);
        return true;
      },
      async listEvents() {
        const { data, error } = await sb().from("login_events")
          .select("event, ip, country, user_agent, created_at")
          .order("created_at", { ascending: false })
          .limit(50);
        if (error) return [];
        return data;
      },
    };
  })();

  // สลับร่างที่บรรทัดนี้บรรทัดเดียวเมื่อใส่ค่า CONFIG ครบแล้ว
  window.TribuneAuth = CONFIG.url ? supabaseAuth : mockAuth;
  window.TribuneAuth.MIN_PASSWORD = MIN_PASSWORD;
})();
