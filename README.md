# VR Ship Tracker

Live AIS ship tracking on a 2D globe and in virtual reality (WebXR / A-Frame),
fed by a small local relay that bridges the browser to an AIS data provider.
The same view can be explored on a desktop screen or in a headset (built and
tested for Meta Quest 3S).

## Features

- **One continuous map.** Zoom from the whole planet down to standing on the
  water beside a vessel at true 1:1 scale. The globe turns into a flat sea
  automatically once the earth's curvature stops being visible.
- **Enter VR from the map.** **ENTER VR** takes the view you are looking at into
  the headset, at the same place and zoom, and brings you back where you left.
- **Shared vessel registry.** The flat map, the globe's dot cloud and the
  true-scale 3D hulls all draw from one registry, one set of filters and one
  playback clock, so switching views is instant.
- **Vessel details and comparison.** Detail card per vessel, a comparison set
  shown side by side in a dash, and a close-up view that cycles through the
  selected vessels.
- **Traffic analysis.** Vessel routes, course projection cones with
  closest-point-of-approach markers, traffic density columns and busiest-area
  beacons.
- **History playback.** The relay logs incoming messages locally; any window of
  that log can be replayed through the same pipeline, and the last six hours of
  routes are preloaded on start.
- **Scales to busy feeds.** Dense views are aggregated into count blobs, and
  per-frame work is limited to the vessels actually on screen.

## Setup

Requires Node.js 18 or newer.

```
npm install
node relay.js
```

Open `http://localhost:3333/`, pick a data source in the connect dialog, then
open the tracker:

- `ais_data_stream.html` — connection manager and live event log
- `ship_tracker.html` — the tracker (2D map and VR)

Both pages run on the same origin (served by `relay.js`) and share live data
over a `BroadcastChannel`.

A `Dockerfile` is included; the container serves everything on port 8080.

## Controls

**Desktop:** drag to rotate, mouse wheel to zoom, click a vessel or the sea to
descend one zoom step. In the 3D view: WASD to move, right-drag or wheel to
zoom.

**VR (Quest controllers):**

| Input | Action |
|---|---|
| Left stick | Up/down to climb and descend, left/right to strafe (click: toggle fly / teleport) |
| Right stick | Turn left/right, look up/down |
| Grip + pull | Grab the world and zoom |
| Both grips | Drag and scale the world |
| Right trigger | Select a vessel, the sea or the planet |
| A | Vessel details; on a selected vessel, open the close-up |
| B | Back (close-up, then selection) |
| X | Add the vessel under the left pointer to the comparison |
| Y / **M** button | Menu: filters, cones, density, playback, comfort settings |

A **LOOK SNAP** comfort setting in the menu switches to snap turning without
pitch. Head-up panel positions and sizes can be tuned in the `HUD` block at the
top of the script in `ship_tracker.html`.

## Data sources

You bring your own credentials — this project never stores or ships anyone's
API key. Each provider is governed by its own terms of use.

| Provider | Credentials needed | Coverage |
|---|---|---|
| [aisstream.io](https://aisstream.io) | Your own API key | Global |
| [BarentsWatch](https://developer.barentswatch.no) | Your own OAuth client id/secret | Norway / Barents Sea |
| [Digitraffic](https://www.digitraffic.fi/en/marine-traffic/) | None | Finland / Baltic Sea |

### Recorded datasets (study sessions)

For controlled comparisons, a time window of the relay's message log can be
saved as a named dataset and replayed as a data source, so every session sees
identical traffic:

```
curl -X POST http://localhost:3333/api/dataset/save \
  -d '{"name":"study-a","since":"2026-09-14T10:00:00Z","until":"2026-09-14T10:30:00Z"}'
curl http://localhost:3333/api/dataset/list
```

`since`/`until` take epoch milliseconds or ISO dates; an existing name is only
replaced with `"overwrite": true`. Datasets are stored in `datasets/<name>.jsonl`.

Pick **Recorded dataset** in the connect dialog, choose the dataset, and
optionally a speed-up and looping. The replay keeps the original message timing
(divided by the speed) and is not written back to the message log. It is also
available directly at `ws://localhost:3333/v0/stream/recorded?name=<name>&speed=1&loop=0`.

The relay writes received messages to local log files for playback. These
contain provider data and are excluded from version control; do not publish
them.

Map data: [Natural Earth](https://www.naturalearthdata.com) (public domain), via
[world-atlas](https://github.com/topojson/world-atlas). Full attribution and
license details for every data source and third-party library are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## License

MIT — see [LICENSE](LICENSE).
