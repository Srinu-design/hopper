// The portfolio home page (deploy/site/index.html) is the Stitch "Lumio" design, whose Tailwind
// config this is, compiled to a static stylesheet because the page's policy allows no other
// origin. Rebuild after changing the page:
//
//   npx --yes tailwindcss@3.4.17 -c deploy/site-src/tailwind.config.js \
//     -i deploy/site-src/input.css -o deploy/site/site.css --minify
//
// One change from the design: "muted" is darker (#8C8880 is 3.3:1 on the page, under WCAG AA).
module.exports = {
  content: ["./deploy/site/index.html"],
  theme: {
    extend: {
      colors: {
        "page-bg": "#FAF9F5",
        "panel-bg": "#F0EFEB",
        "soft-card": "#E9E8E4",
        "soft-card-2": "#F4F3EF",
        text: "#1B1B1B",
        muted: "#6B675F",
        line: "#D9D7D0",
        black: "#181818",
        "white-card": "#FFFDF8",
      },
      borderRadius: {
        DEFAULT: "0.125rem",
        lg: "0.25rem",
        xl: "0.5rem",
        full: "0.75rem",
        "3xl": "2rem",
      },
      spacing: {
        sm: "16px",
        md: "32px",
        xl: "128px",
        lg: "64px",
        margin: "40px",
        xs: "8px",
      },
      fontFamily: {
        label: ["Helvetica", "Arial", "sans-serif"],
        "body-lg": ["Helvetica", "Arial", "sans-serif"],
        display: ["Helvetica", "Arial", "sans-serif"],
        h3: ["Helvetica", "Arial", "sans-serif"],
        "body-md": ["Helvetica", "Arial", "sans-serif"],
      },
      fontSize: {
        label: ["12px", { lineHeight: "1", letterSpacing: "0.05em", fontWeight: "600" }],
        "body-lg": ["18px", { lineHeight: "1.6", letterSpacing: "0", fontWeight: "400" }],
        display: ["82px", { lineHeight: "1.05", letterSpacing: "-0.04em", fontWeight: "700" }],
        h2: ["32px", { lineHeight: "1.2", letterSpacing: "-0.02em", fontWeight: "600" }],
        h3: ["24px", { lineHeight: "1.3", letterSpacing: "-0.01em", fontWeight: "600" }],
      },
    },
  },
};
