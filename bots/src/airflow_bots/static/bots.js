// Small enhancements for the Bots pages. Every page also works without JavaScript:
// forms post and redirect, links navigate. No dependencies and no build step.
(() => {
  "use strict";
  const root = document.documentElement;
  const all = (selector, scope = document) => [...scope.querySelectorAll(selector)];
  root.classList.add("js");

  // ------------------------------------------------------------ theme and font
  // Inside Airflow the page follows Airflow's light/dark switch and borrows its font;
  // opened on its own it follows the operating system.
  let host = null;
  try {
    if (window.parent !== window) host = window.parent.document;
  } catch {
    host = null; // a different origin: keep our own defaults
  }
  const systemDark = matchMedia("(prefers-color-scheme: dark)");
  const applyTheme = () => {
    const dark = host ? host.documentElement.classList.contains("dark") : systemDark.matches;
    root.dataset.theme = dark ? "dark" : "light";
  };
  applyTheme();
  if (host) new MutationObserver(applyTheme).observe(host.documentElement, { attributes: true, attributeFilter: ["class"] });
  else systemDark.addEventListener("change", applyTheme);

  function borrowFonts() {
    const rules = [];
    for (const sheet of host.styleSheets) {
      let list;
      try { list = sheet.cssRules; } catch { continue; }
      for (const rule of list) {
        if (rule.type !== CSSRule.FONT_FACE_RULE) continue;
        rules.push(rule.cssText.replace(/url\((["']?)([^"')]+)\1\)/g,
          (_, quote, url) => `url("${new URL(url, sheet.href || host.baseURI)}")`));
      }
    }
    if (rules.length) document.head.append(Object.assign(document.createElement("style"), { textContent: rules.join("\n") }));
  }

  // ------------------------------------------------------------ feedback
  function toast(message, kind = "ok") {
    let box = document.querySelector(".toasts");
    if (!box) {
      box = Object.assign(document.createElement("div"), { className: "toasts" });
      box.setAttribute("role", "status");
      document.body.append(box);
    }
    const item = Object.assign(document.createElement("div"), { className: `toast ${kind}`, textContent: message });
    box.append(item);
    setTimeout(() => item.remove(), kind === "error" ? 7000 : 3500);
  }

  function confirmDialog(message, action) {
    const dialog = Object.assign(document.createElement("dialog"), { className: "confirm" });
    dialog.innerHTML = '<p></p><form method="dialog"><button class="btn" value="cancel">Cancel</button>'
      + '<button class="btn danger-solid" value="ok"></button></form>';
    dialog.querySelector("p").textContent = message;
    dialog.querySelector("[value=ok]").textContent = action;
    document.body.append(dialog);
    dialog.showModal();
    return new Promise((resolve) => dialog.addEventListener("close", () => {
      resolve(dialog.returnValue === "ok");
      dialog.remove();
    }));
  }

  // ------------------------------------------------------------ unsaved edits
  const dirty = new Set();
  function markDirty(form, on) {
    form.classList.toggle("dirty", on);
    if (on) dirty.add(form);
    else dirty.delete(form);
    for (const button of all(".save", form)) button.disabled = !on;
  }

  document.addEventListener("input", (event) => {
    const form = event.target.form;
    if (form?.hasAttribute("data-track")) markDirty(form, true);
  });
  document.addEventListener("reset", (event) => {
    const form = event.target;
    if (!form.hasAttribute("data-track")) return;
    if (form.hasAttribute("data-draft")) {
      // The server rendered this form with the refused edit; the saved values are on a fresh page.
      event.preventDefault();
      load(location.pathname, {}, form.closest("[data-region]")).catch(() => {});
      return;
    }
    setTimeout(() => markDirty(form, false));
  });
  window.addEventListener("beforeunload", (event) => {
    if (dirty.size) event.preventDefault();
  });

  // ------------------------------------------------------------ page updates
  // Fetch a page and swap it in place, so a save keeps your scroll position and context.
  // Given a region (an element with data-region and an id), only that region and the
  // regions marked data-region="always" are replaced; unsaved edits elsewhere survive.
  async function load(url, init, region) {
    const response = await fetch(url, { credentials: "same-origin", ...init });
    const doc = new DOMParser().parseFromString(await response.text(), "text/html");
    if (!response.ok) {
      toast(doc.querySelector("[data-error]")?.textContent.trim() || `The server answered ${response.status}.`, "error");
      return null;
    }
    const next = doc.querySelector("main");
    if (!next) return location.assign(response.url);
    const replacement = region?.id && doc.getElementById(region.id);
    const changed = [];
    if (replacement) {
      region.replaceWith(replacement);
      changed.push(replacement);
      for (const other of all('[data-region="always"][id]', doc)) {
        const current = document.getElementById(other.id);
        if (current) {
          current.replaceWith(other);
          changed.push(other);
        }
      }
    } else {
      document.querySelector("main").replaceWith(next);
      document.title = doc.title;
      changed.push(next);
    }
    for (const form of dirty) if (!form.isConnected) dirty.delete(form);
    changed.forEach(enhance);
    return changed[0];
  }

  document.addEventListener("submit", async (event) => {
    const form = event.target;
    if (form.method !== "post") return;
    event.preventDefault();
    const button = event.submitter;
    const question = button?.dataset.confirm || form.dataset.confirm;
    if (question && !(await confirmDialog(question, button?.dataset.confirmAction || "Confirm"))) return;
    const body = new FormData(form, button);
    button?.setAttribute("aria-busy", "true");
    try {
      const updated = await load(form.action, { method: "POST", body }, form.closest("[data-region]"));
      updated?.querySelector(".form-status.error")?.scrollIntoView({ block: "nearest", behavior: "smooth" });
    } catch {
      toast("Could not reach the server; nothing was changed.", "error");
    } finally {
      button?.removeAttribute("aria-busy");
    }
  });

  // Whole table rows open their detail page; links and controls inside keep working.
  document.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-href]");
    if (!row || event.target.closest("a, button, input, select, textarea, summary")) return;
    if (getSelection().toString()) return;
    location.assign(row.dataset.href);
  });

  // ------------------------------------------------------------ per-page behaviour
  const units = [["year", 31536000], ["month", 2592000], ["week", 604800], ["day", 86400], ["hour", 3600], ["minute", 60]];
  const relative = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  const local = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });
  function times(scope) {
    for (const el of all("time[datetime]", scope)) {
      const date = new Date(el.getAttribute("datetime"));
      if (Number.isNaN(date.getTime())) continue;
      el.title = local.format(date);
      if (el.hasAttribute("data-absolute")) {
        el.textContent = local.format(date);
        continue;
      }
      const seconds = (date - Date.now()) / 1000;
      const unit = units.find(([, size]) => Math.abs(seconds) >= size);
      el.textContent = unit ? relative.format(Math.round(seconds / unit[1]), unit[0])
        : seconds > 0 ? "in a moment" : "just now";
    }
  }

  function tabs(scope) {
    for (const group of all("[data-tabs]", scope)) {
      const buttons = all("[data-tab]", group);
      const show = (name) => {
        for (const b of buttons) b.setAttribute("aria-selected", String(b.dataset.tab === name));
        for (const panel of all("[data-panel]", group)) panel.hidden = panel.dataset.panel !== name;
      };
      for (const b of buttons) b.addEventListener("click", () => show(b.dataset.tab));
      if (buttons.length) show(buttons[0].dataset.tab);
    }
  }

  function autoRefresh(main) {
    const seconds = Number(main.dataset.refresh);
    if (!seconds) return;
    const timer = setInterval(() => {
      if (!main.isConnected) return clearInterval(timer);
      if (document.hidden || dirty.size || document.querySelector("dialog[open]")) return;
      load(location.href).catch(() => {});
    }, seconds * 1000);
  }

  function enhance(scope) {
    times(scope);
    tabs(scope);
    for (const form of all("form[data-track]", scope)) markDirty(form, form.classList.contains("dirty"));
    for (const note of all("[data-toast]", scope)) {
      toast(note.textContent.trim());
      note.remove();
    }
    const main = scope.matches?.("main") ? scope : scope.querySelector?.("main");
    if (main) autoRefresh(main);
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (host) borrowFonts();
    enhance(document);
    setInterval(() => times(document), 30000);
  });
})();
