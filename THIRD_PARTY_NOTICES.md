# Third-party notices

This project (MIT-licensed, see LICENSE) loads or bundles the following
third-party code and data. Their own licenses apply to their own files.

## Code

| Component | How it's used | License |
|---|---|---|
| [ws](https://github.com/websockets/ws) 8.x | npm dependency of `relay.js` (installed into `node_modules/`, not committed) | MIT |
| [A-Frame](https://aframe.io) 1.5.0 | Loaded from the `aframe.io` CDN in `ship_tracker.html`; includes its build of [three.js](https://threejs.org) | MIT (A-Frame and three.js) |
| [d3](https://d3js.org) 7 | Loaded from the jsDelivr CDN in `ship_tracker.html` | ISC |
| [topojson-client](https://github.com/topojson/topojson-client) 3 | Loaded from the jsDelivr CDN in `ship_tracker.html` | ISC |

None of these libraries are copied into this repository; they are fetched at
runtime from their CDNs.

d3, topojson-client and world-atlas are copyright Mike Bostock (see each
package's own LICENSE file for its exact notice) and licensed under the ISC
License:

```
Licensed under the ISC License.
Permission to use, copy, modify, and/or distribute this software for any purpose
with or without fee is hereby granted, provided that the above copyright notice
and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
THIS SOFTWARE.
```

## Map data

Loaded at runtime in `ship_tracker.html`; nothing is stored in this repository.

| Data | Source | License |
|---|---|---|
| Country polygons, coastlines, borders (1:110m, 1:50m) | [world-atlas](https://github.com/topojson/world-atlas) 2 via jsDelivr, built from Natural Earth | ISC (packaging), public domain (data) |
| Land and minor islands (1:10m), rivers and lake centrelines, lakes (1:50m), ports (1:10m) | [Natural Earth](https://www.naturalearthdata.com) GeoJSON from [nvkelso/natural-earth-vector](https://github.com/nvkelso/natural-earth-vector) via jsDelivr | Public domain |

Natural Earth: "Made with Natural Earth. Free vector and raster map data @
naturalearthdata.com." Attribution is not required by its terms, but given here.

Ocean and sea label positions and the MMSI country-code (MID) table in
`ship_tracker.html` were compiled for this project from public reference
information.

## Live AIS data sources

`ais_data_stream.html` / `relay.js` can connect to three upstream AIS providers.
Each is governed by **its own** terms of use, separate from this project's MIT
license. This repository contains no AIS data: the relay's local logs
(`stream_log.txt`, `ais_message_log.jsonl`) are excluded from version control
and must not be published.

- **[aisstream.io](https://aisstream.io)** — requires the user's own API key,
  entered client-side and never stored by this project. Users are responsible
  for complying with aisstream.io's terms, including any limits on storing or
  redistributing the data.
- **[BarentsWatch](https://developer.barentswatch.no)** (Norwegian Coastal
  Administration) — requires the user's own registered OAuth client id/secret.
  See [barentswatch.no/om/api-vilkar](https://www.barentswatch.no/om/api-vilkar)
  for the terms and the attribution they require when the data is shown.
- **[Digitraffic](https://www.digitraffic.fi/en/marine-traffic/)**
  (Fintraffic) — open data, no account needed. Licensed
  **[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)**.

  > Source: Fintraffic / digitraffic.fi, license CC 4.0 BY

This project never holds or transmits anyone's aisstream.io or BarentsWatch
credentials on their behalf — each user supplies and uses their own, directly
from their browser session, under their own agreement with that provider.
