const CACHE = "econ-monitor-v3";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (e) => {
  if (e.request.method !== "GET") return;

  const url = new URL(e.request.url);
  // หน้าบัญชีผู้ใช้ห้ามเสิร์ฟจาก cache เด็ดขาด — คีย์กับตรรกะการเข้าสู่ระบบ
  // ต้องเป็นของสดเสมอ ไม่งั้นคนที่เคยเปิดจะค้างอยู่กับโค้ดรุ่นเก่าตลอดไป
  if (url.origin === location.origin && url.pathname.includes("/auth/")) return;

  const isNavigation = e.request.mode === "navigate" || e.request.destination === "document";
  // ข้อมูลกราฟถูกเขียนใหม่ทุกรอบ build — ถ้าเสิร์ฟจาก cache ก่อนจะค้างของเก่าถาวร
  const isData = url.origin === location.origin && url.pathname.endsWith(".json");

  if (isNavigation || isData) {
    // ต้อง no-store ไม่ใช่ fetch เปล่าๆ — fetch ธรรมดายังกิน HTTP cache ของเบราว์เซอร์อยู่
    // (GitHub Pages ส่ง max-age มาด้วย) ทำให้ไฟล์ที่ build ใหม่แล้วยังได้ของเก่าไปหลายนาที
    // เคยทำให้งบการเงินที่ขยายเป็น 5 ปีแล้วยังโชว์ 4 ปีบนเว็บจริง
    e.respondWith(
      fetch(new Request(e.request.url, { cache: "no-store", credentials: "same-origin" }))
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(e.request, copy));
          return res;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  e.respondWith(
    caches.match(e.request).then((cached) => {
      if (cached) return cached;
      return fetch(e.request).then((res) => {
        const copy = res.clone();
        caches.open(CACHE).then((c) => c.put(e.request, copy));
        return res;
      });
    })
  );
});
