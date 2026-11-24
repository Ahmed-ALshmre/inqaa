(function () {
  const statusBadge = document.getElementById("hrStatusBadge");
  const countdownText = document.getElementById("hrCountdownText");
  const statTotal = document.getElementById("hrStatTotal");
  const statProcessed = document.getElementById("hrStatProcessed");
  const statResolved = document.getElementById("hrStatResolved");
  const statRemaining = document.getElementById("hrStatRemaining");
  const msgBox = document.getElementById("hrMessage");
  const tableBody = document.getElementById("hrTableBody");
  const tableHint = document.getElementById("hrTableHint");
  const startBtn = document.getElementById("hrStartBtn");
  const stopBtn = document.getElementById("hrStopBtn");
  const refreshBtn = document.getElementById("hrRefreshBtn");

  let pollTimer = null;

  function apiUrl(path) {
    return typeof adminApi === "function" ? adminApi(path) : path;
  }

  function storeLink(senderId) {
    const params = new URLSearchParams(window.location.search);
    const sid = params.get("store_id") || (typeof getActiveStoreId === "function" ? getActiveStoreId() : "");
    return sid
      ? `/?store_id=${encodeURIComponent(sid)}&sender_id=${encodeURIComponent(senderId)}`
      : `/?sender_id=${encodeURIComponent(senderId)}`;
  }

  function esc(str) {
    return String(str || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function stateLabel(st) {
    switch (st) {
      case "running": return { text: "قيد المعالجة الآن", cls: "bg-primary" };
      case "waiting": return { text: "بانتظار دقيقة للمحادثة التالية", cls: "bg-info text-dark" };
      case "completed": return { text: "مكتملة", cls: "bg-success" };
      case "stopped": return { text: "متوقفة يدوياً", cls: "bg-warning text-dark" };
      default: return { text: "جاهز للفحص", cls: "bg-secondary" };
    }
  }

  function itemBadge(st) {
    switch (st) {
      case "processing": return '<span class="badge bg-primary">جاري المعالجة...</span>';
      case "resolved": return '<span class="badge bg-success">✅ تم الحل والرد</span>';
      case "needs_human": return '<span class="badge bg-warning text-dark">⚠️ بقيت للبشري</span>';
      case "skipped": return '<span class="badge bg-secondary">تم التخطي</span>';
      default: return '<span class="badge bg-light text-dark border">بانتظار الدور</span>';
    }
  }

  function render(data) {
    const run = data.run || null;
    const isActive = run && (run.status === "running" || run.status === "waiting");
    const items = run ? (run.items || []) : (data.preview_candidates || []);

    const total = run ? (run.total || 0) : items.length;
    const processed = run ? (run.processed || 0) : 0;
    const resolved = run ? (run.resolved || 0) : 0;
    const remaining = run ? (run.remaining_human || 0) : 0;

    statTotal.textContent = total;
    statProcessed.textContent = processed;
    statResolved.textContent = resolved;
    statRemaining.textContent = remaining;

    const st = stateLabel(run ? run.status : "idle");
    statusBadge.className = "badge fs-6 " + st.cls;
    statusBadge.textContent = st.text;

    if (run && run.status === "waiting" && run.wait_seconds_left > 0) {
      countdownText.textContent = `الانتظار ${run.wait_seconds_left} ثانية قبل معالجة المحادثة التالية...`;
    } else if (run && run.status === "running") {
      countdownText.textContent = `معالجة المحادثة ${Math.min(processed + 1, total)} من ${total}...`;
    } else if (run && run.finished_at) {
      countdownText.textContent = `انتهت المعالجة (${resolved} تم حلها، ${remaining} بقيت للبشري).`;
    } else {
      countdownText.textContent = total > 0
        ? `تم العثور على ${total} محادثة متوقفة آخر 24 ساعة جاهزة للمعالجة.`
        : "لا توجد محادثات متوقفة تحتاج معالجة في آخر 24 ساعة.";
    }

    startBtn.disabled = isActive || (!run && total === 0);
    stopBtn.classList.toggle("d-none", !isActive);

    if (run && run.error) {
      msgBox.innerHTML = `<div class="alert alert-danger py-2 mb-0">${esc(run.error)}</div>`;
    } else {
      msgBox.innerHTML = "";
    }

    tableHint.textContent = items.length ? `${items.length} محادثة` : "";

    if (!items.length) {
      tableBody.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-4">لا توجد محادثات متوقفة بسبب التدخل البشري أو الرصيد في آخر 24 ساعة.</td></tr>';
    } else {
      tableBody.innerHTML = items.map((item, idx) => {
        const name = esc(item.customer_name || item.sender_id);
        const link = storeLink(item.sender_id);
        return `
          <tr>
            <td>${idx + 1}</td>
            <td><a href="${link}" target="_blank" class="fw-semibold text-decoration-none">${name}</a></td>
            <td class="small text-muted">${esc(item.reason || "توقف AI / تدخل بشري")}</td>
            <td class="small" style="max-width:260px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${esc(item.last_message)}">${esc(item.last_message || "—")}</td>
            <td>${itemBadge(item.status)}</td>
            <td class="small">${esc(item.detail || "")}</td>
          </tr>
        `;
      }).join("");
    }

    if (isActive && !pollTimer) {
      pollTimer = setInterval(fetchStatus, 3000);
    } else if (!isActive && pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  async function fetchStatus() {
    try {
      const res = await fetch(apiUrl("/api/settings/handoff-recovery/status"));
      const data = await res.json();
      if (data.ok) render(data);
    } catch (e) {
      msgBox.innerHTML = '<div class="alert alert-danger py-2 mb-0">تعذر تحديث الحالة.</div>';
    }
  }

  startBtn.addEventListener("click", async () => {
    startBtn.disabled = true;
    msgBox.innerHTML = '<div class="alert alert-info py-2 mb-0">جاري بدء المعالجة...</div>';
    try {
      const res = await fetch(apiUrl("/api/settings/handoff-recovery/start"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ hours: 24, interval_seconds: 60 }),
      });
      const data = await res.json();
      if (!data.ok) {
        msgBox.innerHTML = `<div class="alert alert-danger py-2 mb-0">${esc(data.error || "تعذر البدء")}</div>`;
        startBtn.disabled = false;
        return;
      }
      render(data);
    } catch (e) {
      msgBox.innerHTML = '<div class="alert alert-danger py-2 mb-0">حدث خطأ أثناء التشغيل.</div>';
      startBtn.disabled = false;
    }
  });

  stopBtn.addEventListener("click", async () => {
    stopBtn.disabled = true;
    try {
      const res = await fetch(apiUrl("/api/settings/handoff-recovery/stop"), { method: "POST" });
      const data = await res.json();
      if (data.ok) render(data);
    } finally {
      stopBtn.disabled = false;
    }
  });

  refreshBtn.addEventListener("click", fetchStatus);
  fetchStatus();
})();
