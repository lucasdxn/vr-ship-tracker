# VR Ship Tracker

Live AIS ship tracking, visualized on a 3D globe and in VR (WebXR/A-Frame),
fed by a small local relay that bridges the browser to any of four AIS data
providers.

## Setup

```
npm install
node relay.js
```

Then open `http://localhost:3333/`. Pick a data source in the connect modal —
see [Data sources](#data-sources) below for what each one needs.

- `ais_data_stream.html` — connection manager + live event log
- `ship_tracker.html` — the tracker

Both run on the same origin (served by `relay.js`) and share live data over a
`BroadcastChannel`. (The old `globe_ship_tracker.html` and
`vr_ship_tracker.html` were merged into the tracker; those URLs still serve it.)

### The tracker

There is one map. **ENTER VR** puts the view you are already looking at into a
headset, at the same place and the same zoom, and brings it back where you left
it — there is no separate "3D mode" to find first, and no second page.

Clicking is how you go deeper: click a vessel or a patch of sea and the view
descends a rung, on the flat map and in the headset alike. The zoom is one
continuous range from the whole planet down to standing on the water beside a
hull. The sphere becomes a flat sea automatically once the earth's curvature
stops being visible, and the sea is re-centred under you every frame, so there
is no edge to reach at any zoom or distance.

Everything is drawn from one shared vessel registry: one `BroadcastChannel`
listener, one set of filters, one playback clock, painted by three renderers
(the flat canvas, the globe's dot cloud, and true-scale 3D hulls). Whichever is
off screen keeps its state current, so switching is instant rather than a
reload. The coastline is the same `world-atlas` / Natural Earth data in both,
at the same detail tier.

Inside a headset no DOM renders at all, so the controls that matter there are
built as in-scene panels: a wrist menu (the **M** button, or X on the left
controller) carrying the vessel-type filters and legend, course cones, density
columns, live/playback and save/restore view; a ship card with a
"zoom to real scale" jump; a controls reference; and a recentre button.

## Data sources

You bring your own credentials — this project never stores or ships anyone's
API key. Each provider is governed by its own terms of use:

| Provider | Credentials needed | Coverage |
|---|---|---|
| [aisstream.io](https://aisstream.io) | Your own API key | Global |
| [BarentsWatch](https://developer.barentswatch.no) | Your own OAuth client id/secret | Norway / Barents Sea (free tier) |
| [Kpler](https://developers.kpler.com/spec/ais) | Your own bearer token (commercial account) | Global |
| [Digitraffic](https://www.digitraffic.fi/en/marine-traffic/) | None | Finland / Baltic Sea |

Full attribution and license details for each source, plus every third-party
library this project loads, are in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## License

MIT — see [LICENSE](LICENSE).
