// Runs before the page is drawn: a saved light or dark choice wins over the system setting,
// so the page never flashes the wrong theme. app.js switches it and saves the choice.
"use strict";
try {
  const saved = localStorage.getItem("hopper.theme");
  if (saved === "light" || saved === "dark") document.documentElement.dataset.theme = saved;
} catch {
  // storage blocked: follow the system setting
}
