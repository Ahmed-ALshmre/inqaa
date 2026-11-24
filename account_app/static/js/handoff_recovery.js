(function () {
  const statusBadge = document.getElementById("hrStatusBadge");
  const countdownText = document.getElementById("hrCountdownText");
  const statTotal = document.getElementById("hrStatTotal");
  const statProcessed = document.getElementById("hrStatProcessed");
  const statResolved = document.getElementById("hrStatResolved");
  const statRemaining = document.getElementById("hrStatRemaining");
  const progressBar = document.getElementById("hrProgressBar");
  const msgBox = document.getElementById("hrMessage");
  const tableBody = document.getElementById("hrTableBody");
  const mobileList = document.getElementById("hrMobileList");
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

  function initialChar(name) {
    const s = String(name || "ز").trim();
    return s ? s[0].toUpperCase() : "ز";
  }

  function reasonIcon(reason) {
    const r = String(reason || "").toLowerCase();
    if (r.includes("402") || r.includes("credit") || r.includes("رصيد")) return "bi-credit-card-2-back-fill text-danger";
    if (r.includes("image") || r.includes("صور")) return "bi-image-fill text-info";
    if (r.includes("globally_disabled") || r.includes("متوقف")) return "bi-power text-secondary";
    if (r.includes("order") || r.includes("حجز") || r.includes("طلب")) return "bi-bag-exclamation-fill text-warning";
    return "bi-person-raised-hand text-primary";
  }

  function stateLabel(st) {
    switch (st) {
      case "running":
        return { text: "قيد المعالجة الآن", cls: "hr-status-running", icon: "bi-arrow-repeat hr-spin" };
      case "waiting":
        return { text: "انتظار دقيقة للمحادثة التالية", cls: "hr-status-waiting", icon: "bi-hourglass-split" };
      case "completed":
        return { text: "مكتملة", cls: "hr-status-completed", icon: "bi-check-circle-fill" };
      case "stopped":
        return { text: "متوقفة يدوياً", cls: "hr-status-stopped", icon: "bi-pause-circle-fill" };
      default:
        return { text: "جاهز للفحص", cls: "hr-status-idle", icon: "bi-radar" };
    }
  }

  function itemBadge(st) {
    switch (st) {
      case "processing":
        return '<span class="hr-item-badge hr-b-processing"><i class="bi bi-arrow-repeat hr-spin"></i> جاري المعالجة</span>';
      case "resolved":
        return '<span class="hr-item-badge hr-b-resolved"><i class="bi bi-check-circle-fill"></i> تم الحل والرد</span>';
      case "needs_human":
        return '<span class="hr-item-badge hr-b-human"><i class="bi bi-exclamation-triangle-fill"></i> بقيت للبشري</span>';
      case "skipped":
        return '<span class="hr-item-badge hr-b-skipped"><i class="bi bi-skip-forward-fill"></i> تم التخطي</span>';
      default:
        return '<span class="hr-item-badge hr-b-pending"><i class="bi bi-clock"></i> بانتظار الدور</span>';
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

    if (progressBar) {
      const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;
      progressBar.style.width = `${pct}%`;
    }

    const st = stateLabel(run ? run.status : "idle");
    statusBadge.className = "hr-status-pill " + st.cls;
    statusBadge.innerHTML = `<i class="bi ${st.icon}"></i> <span>${st.text}</span>`;

    if (run && run.status === "waiting" && run.wait_seconds_left > 0) {
      countdownText.innerHTML = `<i class="bi bi-clock-history text-primary"></i> الانتظار <strong>${run.wait_seconds_left}</strong> ثانية قبل المحادثة التالية...`;
    } else if (run && run.status === "running") {
      countdownText.innerHTML = `<i class="bi bi-lightning-charge-fill text-primary"></i> معالجة المحادثة <strong>${Math.min(processed + 1, total)}</strong> من <strong>${total}</strong>...`;
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
      msgBox.innerHTML = `<div class="alert alert-danger py-2 mb-0 d-flex align-items-center gap-2"><i class="bi bi-exclamation-octagon-fill"></i><span>${esc(run.error)}</span></div>`;
    } else {
      msgBox.innerHTML = "";
    }

    tableHint.textContent = `${items.length} محادثة`;

    if (!items.length) {
      const emptyHtml = '<i class="bi bi-check2-circle fs-4 d-block mb-1 text-success"></i>لا توجد محادثات متوقفة بسبب التدخل البشري أو الرصيد في آخر 24 ساعة.';
      tableBody.innerHTML = `<tr><td colspan="6" class="text-center text-muted py-4">${emptyHtml}</td></tr>`;
      if (mobileList) mobileList.innerHTML = `<div class="text-center text-muted py-4">${emptyHtml}</div>`;
    } else {
      tableBody.innerHTML = items.map((item, idx) => {
        const name = esc(item.customer_name || item.sender_id);
        const link = storeLink(item.sender_id);
        const reason = esc(item.reason || "توقف AI / تدخل بشري");
        const rIcon = reasonIcon(item.reason);
        return `
          <tr>
            <td class="text-muted fw-semibold">${idx + 1}</td>
            <td>
              <a href="${link}" target="_blank" class="d-inline-flex align-items-center gap-2 fw-semibold text-decoration-none text-dark">
                <span class="hr-avatar">${esc(initialChar(item.customer_name))}</span>
                <span>${name}</span>
                <i class="bi bi-box-arrow-up-left small text-muted"></i>
              </a>
            </td>
            <td><span class="hr-reason-pill"><i class="bi ${rIcon}"></i> ${reason}</span></td>
            <td class="small text-secondary" style="max-width:240px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${esc(item.last_message)}">${esc(item.last_message || "—")}</td>
            <td>${itemBadge(item.status)}</td>
            <td class="small text-dark">${esc(item.detail || "—")}</td>
          </tr>
        `;
      }).join("");

      if (mobileList) {
        mobileList.innerHTML = items.map((item, idx) => {
          const name = esc(item.customer_name || item.sender_id);
          const link = storeLink(item.sender_id);
          const reason = esc(item.reason || "توقف AI / تدخل بشري");
          const rIcon = reasonIcon(item.reason);
          return `
            <div class="hr-mcard">
              <div class="hr-mcard-top">
                <a href="${link}" target="_blank" class="d-inline-flex align-items-center gap-2 fw-bold text-decoration-none text-dark">
                  <span class="hr-avatar">${esc(initialChar(item.customer_name))}</span>
                  <span>${idx + 1}. ${name}</span>
                </a>
                ${itemBadge(item.status)}
              </div>
              <div><span class="hr-reason-pill"><i class="bi ${rIcon}"></i> ${reason}</span></div>
              <div class="hr-mcard-msg"><i class="bi bi-chat-quote text-muted ms-1"></i> ${esc(item.last_message || "—")}</div>
              ${item.detail ? `<div class="small text-dark mb-2"><i class="bi bi-info-circle text-primary ms-1"></i> ${esc(item.detail)}</div>` : ""}
              <div class="text-start">
                <a href="${link}" target="_blank" class="btn btn-outline-primary btn-sm rounded-3">
                  <i class="bi bi-chat-dots"></i> فتح المحادثة
                </a>
              </div>
            </div>
          `;
        }).join("");
      }
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
    msgBox.innerHTML = '<div class="alert alert-info py-2 mb-0 d-flex align-items-center gap-2"><i class="bi bi-arrow-repeat hr-spin"></i><span>جاري بدء المعالجة...</span></div>';
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
