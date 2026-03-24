content = '''<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mimir — Admin</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      theme: {
        extend: {
          fontFamily: { sans: ["Inter", "system-ui", "sans-serif"] }
        }
      }
    }
  </script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    body { font-family: "Inter", system-ui, sans-serif; background-color: #13151a; color: #e2e8f0; }
    .threshold-cell { transition: background-color 0.15s, color 0.15s, border-color 0.15s; }
    input[type=number]::-webkit-inner-spin-button,
    input[type=number]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    input[type=number] { -moz-appearance: textfield; }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #1c1f27; }
    ::-webkit-scrollbar-thumb { background: #3a3f4d; border-radius: 3px; }
  </style>
</head>

<body class="h-full antialiased">

  <!-- Header -->
  <header class="sticky top-0 z-40 border-b" style="background:#1c1f27; border-color:#2a2d38;">
    <div class="max-w-screen-xl mx-auto px-6 h-16 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <img src="/static/logo.png" alt="Mimir" class="w-9 h-9 object-contain flex-shrink-0 invert">
        <div>
          <p class="text-sm font-semibold text-white leading-none">Mimir</p>
          <p class="text-xs mt-0.5" style="color:#6b7385;">Admin Dashboard</p>
        </div>
      </div>
      <div class="flex items-center gap-5">
        <div class="flex items-center gap-2">
          <div class="w-7 h-7 rounded-full flex items-center justify-center text-xs font-semibold"
               style="background:#2a2d3e; color:#818cf8;">
            {{ user.name[0].upper() if user.name else "?" }}
          </div>
          <span class="text-sm hidden sm:block" style="color:#9aa0b4;">{{ user.email }}</span>
        </div>
        <a href="/auth/logout" class="text-sm font-medium transition-colors" style="color:#4f5668;"
           onmouseover="this.style.color=\'#9aa0b4\'" onmouseout="this.style.color=\'#4f5668\'">Sign out</a>
      </div>
    </div>
  </header>

  <!-- Main -->
  <main class="max-w-screen-xl mx-auto px-6 py-8">

    <div class="mb-6">
      <h1 class="text-xl font-semibold text-white">Alert Thresholds</h1>
      <p class="text-sm mt-1" style="color:#6b7385;">
        Click any threshold value to edit it. Changes are saved instantly to BigQuery and take effect on the next monitor cycle.
      </p>
    </div>

    <!-- Stats -->
    <div class="grid grid-cols-3 gap-4 mb-6">
      <div class="rounded-xl border p-5" style="background:#1c1f27; border-color:#2a2d38;">
        <p class="text-xs font-medium uppercase tracking-wide" style="color:#6b7385;">Total Metrics</p>
        <p class="text-3xl font-bold text-white mt-1 tabular-nums">{{ total_count }}</p>
      </div>
      <div class="rounded-xl border p-5" style="background:#1c1f27; border-color:#2a2d38;">
        <p class="text-xs font-medium uppercase tracking-wide" style="color:#6b7385;">Active</p>
        <p class="text-3xl font-bold mt-1 tabular-nums" style="color:#34d399;">{{ active_count }}</p>
      </div>
      <div class="rounded-xl border p-5" style="background:#1c1f27; border-color:#2a2d38;">
        <p class="text-xs font-medium uppercase tracking-wide" style="color:#6b7385;">Inactive</p>
        <p class="text-3xl font-bold mt-1 tabular-nums" style="color:#4f5668;">{{ total_count - active_count }}</p>
      </div>
    </div>

    <!-- Table card -->
    <div class="rounded-xl border overflow-hidden shadow-xl" style="background:#1c1f27; border-color:#2a2d38;">

      <!-- Toolbar -->
      <div class="px-6 py-4 border-b flex flex-wrap items-center gap-3" style="border-color:#2a2d38;">
        <div class="relative flex-1 min-w-[200px] max-w-sm">
          <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 pointer-events-none"
               style="color:#4f5668;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
          </svg>
          <input id="search-input" type="text" placeholder="Search metrics..."
            class="w-full pl-9 pr-4 py-2 text-sm rounded-lg border outline-none"
            style="background:#13151a; border-color:#2a2d38; color:#c8cdd8;"
            oninput="filterTable()"
            onfocus="this.style.borderColor=\'#6366f1\'"
            onblur="this.style.borderColor=\'#2a2d38\'">
        </div>
        <select id="filter-direction" onchange="filterTable()"
          class="text-sm rounded-lg border px-3 py-2 outline-none cursor-pointer"
          style="background:#13151a; border-color:#2a2d38; color:#9aa0b4;">
          <option value="">All directions</option>
          <option value="down_is_bad">Drop alert</option>
          <option value="up_is_bad">Rise alert</option>
        </select>
        <select id="filter-status" onchange="filterTable()"
          class="text-sm rounded-lg border px-3 py-2 outline-none cursor-pointer"
          style="background:#13151a; border-color:#2a2d38; color:#9aa0b4;">
          <option value="">All statuses</option>
          <option value="active">Active</option>
          <option value="inactive">Inactive</option>
        </select>
        <p class="text-sm ml-auto" style="color:#4f5668;" id="row-count"></p>
      </div>

      <!-- Table -->
      <div class="overflow-x-auto">
        <table class="w-full text-sm" id="metrics-table">
          <thead>
            <tr class="border-b" style="border-color:#2a2d38; background:#171a21;">
              <th class="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">Metric</th>
              <th class="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">Direction</th>
              <th class="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">
                <span>Pace %</span><span class="block font-normal normal-case" style="color:#3a3f4d;">Intraday</span>
              </th>
              <th class="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">
                <span>DoD %</span><span class="block font-normal normal-case" style="color:#3a3f4d;">Day-over-day</span>
              </th>
              <th class="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">
                <span>WoW %</span><span class="block font-normal normal-case" style="color:#3a3f4d;">Week-over-week</span>
              </th>
              <th class="px-4 py-3 text-center text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">Status</th>
              <th class="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wide" style="color:#4f5668;">Last Updated</th>
            </tr>
          </thead>
          <tbody id="table-body">
            {% for m in metrics %}
            <tr class="group border-b transition-colors" style="border-color:#1e2028;"
                onmouseover="this.style.background=\'#20232c\'" onmouseout="this.style.background=\'\'"
                data-label="{{ m.metric_label | lower }}"
                data-direction="{{ m.direction }}"
                data-enabled="{{ \'active\' if m.enabled else \'inactive\' }}">

              <td class="px-6 py-4">
                <div class="font-medium text-white leading-tight">{{ m.metric_label }}</div>
                {% if m.steep_url %}
                <a href="{{ m.steep_url }}" target="_blank" rel="noopener noreferrer"
                   class="text-xs opacity-0 group-hover:opacity-100 transition-opacity mt-0.5 inline-block"
                   style="color:#6366f1;">View in Steep ↗</a>
                {% endif %}
              </td>

              <td class="px-4 py-4 whitespace-nowrap">
                {% if m.direction == "up_is_bad" %}
                <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium"
                      style="background:#2d1d20; color:#f87171; border:1px solid #3d2022;">
                  <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 15l7-7 7 7"/>
                  </svg>Rise alert
                </span>
                {% else %}
                <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium"
                      style="background:#1a1f35; color:#818cf8; border:1px solid #252b45;">
                  <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M19 9l-7 7-7-7"/>
                  </svg>Drop alert
                </span>
                {% endif %}
              </td>

              {% for comp, val in [("pace", m.pace_threshold), ("dod", m.dod_threshold), ("wow", m.wow_threshold)] %}
              <td class="px-4 py-4 text-center">
                <span class="threshold-cell inline-block min-w-[64px] px-3 py-1.5 rounded-lg text-sm font-medium tabular-nums cursor-pointer border"
                      style="background:#13151a; color:#9aa0b4; border-color:#2a2d38;"
                      data-metric="{{ m.metric_id }}" data-comparison="{{ comp }}"
                      data-value="{{ ((val or 0) * 100) | round(1) }}"
                      onclick="startEdit(this)"
                      onmouseover="if(!this.querySelector(\'input\')){this.style.background=\'#1e2230\';this.style.color=\'#818cf8\';this.style.borderColor=\'#3a3f5c\';}"
                      onmouseout="if(!this.querySelector(\'input\')){this.style.background=\'#13151a\';this.style.color=\'#9aa0b4\';this.style.borderColor=\'#2a2d38\';}"
                      title="Click to edit">
                  {{ ((val or 0) * 100) | round(1) }}%
                </span>
              </td>
              {% endfor %}

              <td class="px-4 py-4 text-center">
                <label class="relative inline-flex items-center cursor-pointer">
                  <input type="checkbox" class="sr-only peer" data-metric="{{ m.metric_id }}"
                         {{ \'checked\' if m.enabled else \'\' }}
                         onchange="toggleMetric(\'{{ m.metric_id }}\', this.checked, this)">
                  <div class="w-10 h-5 rounded-full relative transition-colors"
                       style="background: {{ \'#4f46e5\' if m.enabled else \'#2a2d38\' }};"
                       id="toggle-track-{{ m.metric_id }}">
                    <div class="absolute top-0.5 h-4 w-4 bg-white rounded-full shadow transition-all"
                         style="left: {{ \'22px\' if m.enabled else \'2px\' }};"
                         id="toggle-thumb-{{ m.metric_id }}"></div>
                  </div>
                </label>
              </td>

              <td class="px-6 py-4 text-xs whitespace-nowrap" style="color:#4f5668;">
                {{ m.updated_at[:10] if m.updated_at else "—" }}
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>

      <div id="empty-state" class="hidden py-16 text-center">
        <svg class="mx-auto w-10 h-10 mb-3" style="color:#2a2d38;" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
            d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
        </svg>
        <p class="text-sm" style="color:#4f5668;">No metrics match your search.</p>
      </div>
    </div>

  </main>

  <div id="toast-container" class="fixed top-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

  <script>
    function showToast(message, isError = false) {
      const container = document.getElementById("toast-container");
      const el = document.createElement("div");
      el.style.cssText = "pointer-events:auto;display:flex;align-items:center;gap:10px;padding:12px 16px;border-radius:12px;font-size:14px;font-weight:500;border:1px solid;transition:all 0.3s;box-shadow:0 8px 24px rgba(0,0,0,0.4);" + (isError ? "background:#2d1d20;color:#f87171;border-color:#3d2022;" : "background:#1a2d25;color:#34d399;border-color:#1e3d30;");
      const icon = isError
        ? "<path stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\" stroke-width=\\"2\\" d=\\"M6 18L18 6M6 6l12 12\\"/>"
        : "<path stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\" stroke-width=\\"2\\" d=\\"M5 13l4 4L19 7\\"/>";
      el.innerHTML = `<svg style="width:16px;height:16px;flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24">${icon}</svg><span>${message}</span>`;
      container.appendChild(el);
      setTimeout(() => { el.style.opacity = "0"; el.style.transform = "translateX(0.5rem)"; setTimeout(() => el.remove(), 300); }, 3200);
    }

    let _activeInput = null;

    function startEdit(cell) {
      if (_activeInput) cancelEdit();
      const currentValue = cell.dataset.value;
      cell.innerHTML = `<input type="number" min="0.1" max="100" step="0.1" value="${currentValue}" class="w-[72px] px-2 py-1 text-sm text-center rounded-lg outline-none tabular-nums" style="background:#13151a;border:2px solid #6366f1;color:#e2e8f0;" onkeydown="handleEditKey(event,this)" onblur="commitEdit(this)">`;
      const input = cell.querySelector("input");
      input._cell = cell; input._metricId = cell.dataset.metric;
      input._comparison = cell.dataset.comparison; input._originalValue = currentValue;
      input.select(); _activeInput = input;
    }

    function cancelEdit() {
      if (!_activeInput) return;
      const input = _activeInput; _activeInput = null;
      restoreCell(input._cell, input._originalValue);
    }

    function restoreCell(cell, value) {
      cell.innerHTML = `${parseFloat(value).toFixed(1)}%`;
      cell.dataset.value = value;
    }

    function handleEditKey(e, input) {
      if (e.key === "Enter") { e.preventDefault(); input.blur(); }
      if (e.key === "Escape") { e.preventDefault(); _activeInput = null; restoreCell(input._cell, input._originalValue); }
    }

    async function commitEdit(input) {
      if (_activeInput !== input) return;
      _activeInput = null;
      const newValue = parseFloat(input.value);
      const cell = input._cell, metricId = input._metricId, comparison = input._comparison, originalValue = input._originalValue;
      if (isNaN(newValue) || newValue <= 0 || newValue > 100) { restoreCell(cell, originalValue); showToast("Value must be between 0.1 and 100 (%)", true); return; }
      if (Math.abs(newValue - parseFloat(originalValue)) < 0.005) { restoreCell(cell, originalValue); return; }
      cell.innerHTML = `<span style="color:#6366f1;" class="animate-pulse">${newValue.toFixed(1)}%</span>`;
      try {
        const resp = await fetch(`/api/metrics/${encodeURIComponent(metricId)}/threshold`, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({comparison, value: newValue}) });
        if (!resp.ok) { const err = await resp.json().catch(() => ({})); throw new Error(err.detail || `HTTP ${resp.status}`); }
        cell.dataset.value = newValue; restoreCell(cell, newValue);
        cell.style.background = "#1a2d25"; cell.style.color = "#34d399"; cell.style.borderColor = "#1e3d30";
        setTimeout(() => { cell.style.background = "#13151a"; cell.style.color = "#9aa0b4"; cell.style.borderColor = "#2a2d38"; }, 1600);
        showToast(`${comparison.toUpperCase()} threshold \\u2192 ${newValue.toFixed(1)}%`);
      } catch (err) { restoreCell(cell, originalValue); showToast(`Failed to save: ${err.message}`, true); }
    }

    async function toggleMetric(metricId, enabled, checkbox) {
      checkbox.disabled = true;
      const track = document.getElementById(`toggle-track-${metricId}`);
      const thumb = document.getElementById(`toggle-thumb-${metricId}`);
      const row = checkbox.closest("tr");
      try {
        const resp = await fetch(`/api/metrics/${encodeURIComponent(metricId)}/toggle`, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({enabled}) });
        if (!resp.ok) { const err = await resp.json().catch(() => ({})); throw new Error(err.detail || `HTTP ${resp.status}`); }
        if (track) track.style.background = enabled ? "#4f46e5" : "#2a2d38";
        if (thumb) thumb.style.left = enabled ? "22px" : "2px";
        row.dataset.enabled = enabled ? "active" : "inactive";
        showToast(`Metric ${enabled ? "enabled" : "disabled"}`);
      } catch (err) { checkbox.checked = !enabled; showToast(`Failed: ${err.message}`, true); }
      finally { checkbox.disabled = false; }
    }

    function filterTable() {
      const query = document.getElementById("search-input").value.toLowerCase().trim();
      const direction = document.getElementById("filter-direction").value;
      const status = document.getElementById("filter-status").value;
      const rows = document.querySelectorAll("#table-body tr");
      let visible = 0;
      rows.forEach(row => {
        const show = (!query || row.dataset.label.includes(query)) && (!direction || row.dataset.direction === direction) && (!status || row.dataset.enabled === status);
        row.classList.toggle("hidden", !show);
        if (show) visible++;
      });
      document.getElementById("row-count").textContent = `${visible} of ${rows.length}`;
      document.getElementById("empty-state").classList.toggle("hidden", visible > 0);
    }
    filterTable();
  </script>
</body>
</html>
'''

with open('/home/oliver/discord-bq-bot/templates/admin.html', 'w') as f:
    f.write(content)
print('Done!')
