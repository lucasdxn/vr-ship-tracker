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
- `globe_ship_tracker.html` — 3D globe visualization
- `vr_ship_tracker.html` — VR/WebXR visualization

All three run on the same origin (served by `relay.js`) and share live data
over a `BroadcastChannel`.

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
