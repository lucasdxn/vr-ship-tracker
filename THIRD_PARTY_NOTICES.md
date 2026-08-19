# Third-party notices

This project (MIT-licensed, see LICENSE) loads or bundles the following
third-party code and data. Their own licenses apply to their own files.

## Code

| Component | How it's used | License |
|---|---|---|
| [ws](https://github.com/websockets/ws) 8.20.1 | npm dependency, bundled in `node_modules/ws` | MIT |
| [A-Frame](https://aframe.io) 1.5.0 | Loaded from `aframe.io` CDN in `vr_ship_tracker.html` | MIT |
| [d3](https://d3js.org) | Loaded from CDN in `globe_ship_tracker.html` | ISC |
| [topojson-client](https://github.com/topojson/topojson-client) | Loaded from CDN in `globe_ship_tracker.html` | ISC |
| [world-atlas](https://github.com/topojson/world-atlas) | Loaded from CDN in `globe_ship_tracker.html` | ISC |

```
d3, topojson-client, world-atlas
Copyright 2012-2019 Michael Bostock

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

**[Natural Earth](https://www.naturalearthdata.com)** basemap vectors, loaded
from CDN via `world-atlas`/`topojson` in `globe_ship_tracker.html`.
Public domain — no permission or attribution required, per the Natural Earth
terms of use.

## Live AIS data sources

`ais_data_stream.html` / `relay.js` can connect to any of four upstream AIS
providers. Each is governed by **its own** terms of use, separate from this
project's MIT license:

- **[aisstream.io](https://aisstream.io)** — requires the user's own API key,
  entered client-side and never stored by this project. Users are responsible
  for complying with aisstream.io's own terms.
- **[BarentsWatch](https://developer.barentswatch.no)** — requires the user's
  own registered OAuth client id/secret. Norwegian government open data; see
  [barentswatch.no/om/api-vilkar](https://www.barentswatch.no/om/api-vilkar)
  for their terms.
- **[Kpler](https://developers.kpler.com/spec/ais)** — requires the user's own
  bearer token issued under their own commercial Kpler account/contract. This
  is a paid enterprise data product; this project does not include, cache, or
  redistribute any Kpler response data.
- **[Digitraffic](https://www.digitraffic.fi/en/marine-traffic/)**
  (Fintraffic) — open data, no account needed. Licensed
  **[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)**.

  > Source: Fintraffic / digitraffic.fi, license CC 4.0 BY

This project never holds or transmits anyone's aisstream.io / BarentsWatch /
Kpler credentials on their behalf — each user supplies and uses their own,
directly from their browser session, under their own agreement with that
provider.
