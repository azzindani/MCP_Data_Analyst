# Bundled fonts

The dashboard's looks name these faces. Each is a variable font, Latin subset, WOFF2, taken from
Google Fonts (https://fonts.google.com) and licensed under the SIL Open Font License 1.1 (the licence
text sits beside each file as `<name>.OFL.txt`).

| File | Family | Weights | Used by |
|---|---|---|---|
| `nunito.woff2` | Nunito | 200-1000 | headings of the `lagoon` look |
| `nunito-sans.woff2` | Nunito Sans | 200-1000 | body of the `lagoon` look |
| `source-sans-3.woff2` | Source Sans 3 | 200-900 | `studio` (the default) and `harbor` |
| `sora.woff2` | Sora | 100-800 | headings of the `nocturne` look |
| `manrope.woff2` | Manrope | 200-800 | body of the `nocturne` look |

A page carries the faces its look uses as `data:` URIs inside its own stylesheet, so it needs no
network and looks the same on every machine. Text outside Latin falls back to the system stack named
after the face in `shared/dashboard_looks.py`.
