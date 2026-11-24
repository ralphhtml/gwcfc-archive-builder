# gwcfc-archive-builder

Draws archive satellite pictures for the Guta Forecasting Center radar site (https://radar.gwcfc.net/) on GitHub's
machines and uploads them to the site's parsing server.

- `arc.py --events` builds the Time Machine's named weather events (`events.json`) ahead of time, so they open at once.
- `arc.py --serve` reads the server's list of archive pictures visitors are waiting for every two seconds, draws
  each and uploads it at once.
- `pipeline.bin` is the parsing server's own drawing code, encrypted (AES-256). It is not readable from this repository.

Two repository secrets are needed: `GWCFC_BASE` and `GWCFC_UPLOAD_KEY`. Neither is ever printed, and the workflows
never run on pull requests. Every upload is signed; the server holds only the matching public key.
