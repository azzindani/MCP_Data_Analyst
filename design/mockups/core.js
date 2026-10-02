/* Shared helpers for the three dashboard mockups. Everything reads colors from CSS tokens,
   so a theme switch is a re-render, never a second stylesheet. */
(function () {
  const root = document.documentElement;
  const tok = (name) => getComputedStyle(root).getPropertyValue("--" + name).trim();

  const nf0 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });
  const nf1 = new Intl.NumberFormat("en-GB", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const nf2 = new Intl.NumberFormat("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const fmt = {
    int: (v) => nf0.format(v),
    pct: (v) => nf1.format(v) + "%",
    eur: (v) => "€" + nf2.format(v),
    eurShort: (v) => {
      const a = Math.abs(v);
      if (a >= 1e6) return "€" + nf1.format(v / 1e6) + "M";
      if (a >= 1e3) return "€" + nf1.format(v / 1e3) + "K";
      return "€" + nf0.format(v);
    },
    short: (v) => {
      const a = Math.abs(v);
      if (a >= 1e6) return nf1.format(v / 1e6) + "M";
      if (a >= 1e4) return nf1.format(v / 1e3) + "K";
      return nf0.format(v);
    },
    month: (ym) => {
      const [y, m] = ym.split("-");
      return ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][+m - 1] + " " + y;
    },
  };

  /* A change between two periods, phrased the way a reader checks it.
     `lowerIsBetter` flips the good/bad color, never the arrow. */
  function delta(cur, prev, { points = false, lowerIsBetter = false } = {}) {
    if (prev == null || cur == null) return { text: "n/a", dir: 0, good: null };
    const d = points ? cur - prev : ((cur - prev) / Math.abs(prev)) * 100;
    const dir = Math.abs(d) < 0.05 ? 0 : d > 0 ? 1 : -1;
    const good = dir === 0 ? null : (dir > 0) !== lowerIsBetter;
    const text = (d > 0 ? "+" : d < 0 ? "−" : "") + nf1.format(Math.abs(d)) + (points ? " pts" : "%");
    return { text, dir, good };
  }

  let uid = 0;
  function sparkSVG(values, { stroke, fill, w = 120, h = 36, endDot = true, label = "" } = {}) {
    const vs = values.filter((v) => v != null);
    const min = Math.min(...vs), max = Math.max(...vs);
    const span = max - min || 1;
    const pad = 3;
    const pts = values.map((v, i) => [
      pad + (i / (values.length - 1)) * (w - 2 * pad),
      pad + (1 - (v - min) / span) * (h - 2 * pad),
    ]);
    const line = pts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
    const id = "sg" + uid++;
    const last = pts[pts.length - 1];
    return `<svg class="spark" viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img" aria-label="${label}">
      <defs><linearGradient id="${id}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0" stop-color="${fill || stroke}" stop-opacity=".32"/><stop offset="1" stop-color="${fill || stroke}" stop-opacity="0"/>
      </linearGradient></defs>
      <path d="${line} L${last[0].toFixed(1)} ${h} L${pts[0][0].toFixed(1)} ${h} Z" fill="url(#${id})"/>
      <path d="${line}" fill="none" stroke="${stroke}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
      ${endDot ? `<circle cx="${last[0]}" cy="${last[1]}" r="3" fill="${stroke}"/>` : ""}
    </svg>`;
  }

  function ringSVG(pct, { size = 120, width = 12, color, track, text, sub, textColor, subColor } = {}) {
    const r = (size - width) / 2, c = 2 * Math.PI * r;
    const on = Math.max(0, Math.min(100, pct)) / 100 * c;
    return `<svg class="ring" viewBox="0 0 ${size} ${size}" width="${size}" height="${size}" role="img" aria-label="${text || ""} ${sub || ""}">
      <circle cx="${size / 2}" cy="${size / 2}" r="${r}" fill="none" stroke="${track}" stroke-width="${width}"/>
      <circle cx="${size / 2}" cy="${size / 2}" r="${r}" fill="none" stroke="${color}" stroke-width="${width}"
        stroke-linecap="round" stroke-dasharray="${on} ${c}" transform="rotate(-90 ${size / 2} ${size / 2})"/>
      ${text ? `<text x="50%" y="${sub ? "47%" : "50%"}" text-anchor="middle" dominant-baseline="middle" fill="${textColor}" class="ring-text">${text}</text>` : ""}
      ${sub ? `<text x="50%" y="66%" text-anchor="middle" dominant-baseline="middle" fill="${subColor}" class="ring-sub">${sub}</text>` : ""}
    </svg>`;
  }

  /* Half-circle gauge, 0 to 100, value arc drawn over a track. */
  function gaugeSVG(pct, { w = 150, width = 16, color, track, text, textColor } = {}) {
    const r = (w - width) / 2, cx = w / 2, cy = r + width / 2, h = cy + 4;
    const a = Math.PI * (1 - Math.max(0, Math.min(100, pct)) / 100);
    const x = cx + r * Math.cos(a), y = cy - r * Math.sin(a);
    const arc = (x1, y1) => `M${cx - r} ${cy} A${r} ${r} 0 0 1 ${x1} ${y1}`;
    return `<svg class="gauge" viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img" aria-label="${text || ""}">
      <path d="${arc(cx + r, cy)}" fill="none" stroke="${track}" stroke-width="${width}" stroke-linecap="butt"/>
      <path d="${arc(x.toFixed(2), y.toFixed(2))}" fill="none" stroke="${color}" stroke-width="${width}" stroke-linecap="butt"/>
      ${text ? `<text x="${cx}" y="${cy - 4}" text-anchor="middle" fill="${textColor}" class="gauge-text">${text}</text>` : ""}
    </svg>`;
  }

  /* One Plotly look for every chart: transparent ground, token colors, no mode bar. */
  function baseLayout(extra = {}) {
    const ink = tok("ink"), muted = tok("muted"), line = tok("line"), card = tok("card") || tok("panel");
    const family = tok("font-body") || "system-ui, sans-serif";
    const base = {
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
      font: { family, size: 12, color: muted },
      margin: { l: 48, r: 12, t: 10, b: 34 },
      xaxis: { showgrid: false, zeroline: false, linecolor: line, tickcolor: line, ticks: "", automargin: true, fixedrange: true },
      yaxis: { gridcolor: line, griddash: "dot", zeroline: false, ticks: "", automargin: true, fixedrange: true, separatethousands: true },
      hoverlabel: { bgcolor: card, bordercolor: line, font: { color: ink, family, size: 12 } },
      legend: { orientation: "h", x: 0, y: 1.14, xanchor: "left", font: { color: muted, size: 12 }, bgcolor: "rgba(0,0,0,0)" },
      bargap: 0.38, barcornerradius: 6, hovermode: "x unified", dragmode: false,
    };
    return deepMerge(base, extra);
  }
  function deepMerge(a, b) {
    const out = Array.isArray(a) ? a.slice() : { ...a };
    for (const k in b) {
      out[k] = b[k] && typeof b[k] === "object" && !Array.isArray(b[k]) && a[k] && typeof a[k] === "object" ? deepMerge(a[k], b[k]) : b[k];
    }
    return out;
  }
  const config = { displayModeBar: false, responsive: true, displaylogo: false };
  function plot(el, traces, layout) {
    const node = typeof el === "string" ? document.getElementById(el) : el;
    if (!node || !window.Plotly) return;
    Plotly.react(node, traces, layout, config);
  }

  /* Re-render when the viewer's theme changes: OS setting or an explicit data-theme. */
  function onTheme(fn) {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    mq.addEventListener ? mq.addEventListener("change", fn) : mq.addListener(fn);
    new MutationObserver(fn).observe(root, { attributes: true, attributeFilter: ["data-theme"] });
  }

  /* Segmented control: buttons with data-slice; one active at a time. */
  function bindSlices(container, onPick) {
    const btns = [...container.querySelectorAll("[data-slice]")];
    btns.forEach((b) => b.addEventListener("click", () => {
      btns.forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      onPick(b.dataset.slice);
    }));
  }

  window.MK = { tok, fmt, delta, sparkSVG, ringSVG, gaugeSVG, baseLayout, plot, onTheme, bindSlices, deepMerge };
})();
