// The portfolio home page: the Hopper / Sift tabs over "Explore a project". Without this script
// the tab row stays hidden and both groups of cards show, one after the other.
"use strict";

document.addEventListener("DOMContentLoaded", () => {
  const list = document.querySelector('[role="tablist"]');
  const tabs = [...list.querySelectorAll('[role="tab"]')];

  function select(tab) {
    for (const t of tabs) {
      const on = t === tab;
      t.setAttribute("aria-selected", String(on));
      t.tabIndex = on ? 0 : -1;
      document.getElementById(t.getAttribute("aria-controls")).hidden = !on;
    }
  }

  for (const tab of tabs) {
    tab.addEventListener("click", () => select(tab));
    // Arrow keys move between tabs, as the ARIA tabs pattern expects.
    tab.addEventListener("keydown", (event) => {
      const step = { ArrowRight: 1, ArrowLeft: -1 }[event.key];
      if (!step) return;
      const next = tabs[(tabs.indexOf(tab) + step + tabs.length) % tabs.length];
      select(next);
      next.focus();
    });
  }
  list.hidden = false;
  select(tabs[0]);
});
