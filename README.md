# apoHouze 💊

apoHouze — home medicine app — multi-country PWA with Docker support.

## Quick start

1. Download `docker-compose.yml`
2. Set your country code in `COUNTRY:` (see list below)
3. Run `docker compose up -d`
4. Look up the **setup code** in the container log (Synology: Container Manager → apohouze → Log)
5. Open `http://your-server:3525`, create your account with that code and set up two-step verification

No username or password is stored in the compose file.

## First use: account + two-step verification

1. On first start the app prints a one-time **setup code** in the log. Only someone who can read the log can create the account.
2. You choose a username and a password (min. 10 characters; stored as a bcrypt hash).
3. You must then scan a QR code with an authenticator app (Google Authenticator, Aegis, Authy, 1Password, …) and confirm with a 6-digit code. The app is unusable until this is done.
4. You receive 8 one-time **recovery codes**. Store them safely (they are shown once).
5. From then on every login needs password + code. A recovery code can replace the app code once.

Locked out (lost phone and recovery codes)? Add `RESET_ACCOUNT: "true"` to the compose file, restart once, read the new setup code in the log, set up again, and **remove the variable again**. Your inventory is kept.

## Supported countries

### West-Europa

| Code | Country | Medicine registry source |
|------|---------|--------------------------|
| `BE` | 🇧🇪 Belgium | FAMHP / afmps.be |
| `NL` | 🇳🇱 Netherlands | CBG / geneesmiddeleninformatiebank.nl |
| `DE` | 🇩🇪 Germany | BfArM / gelbe-liste.de |
| `FR` | 🇫🇷 France | ANSM / medicaments.gouv.fr |
| `ES` | 🇪🇸 Spain | AEMPS / cima.aemps.es |
| `IT` | 🇮🇹 Italy | AIFA / farmaci.agenziafarmaco.it |
| `CH` | 🇨🇭 Switzerland | Swissmedic / swissmedicinfo.ch |
| `AT` | 🇦🇹 Austria | BASG / ages.at |
| `LU` | 🇱🇺 Luxembourg | Ministère Santé / sante.public.lu |

### Noord-Europa

| Code | Country | Medicine registry source |
|------|---------|--------------------------|
| `GB` | 🇬🇧 United Kingdom | MHRA / bnf.nice.org.uk |
| `IE` | 🇮🇪 Ireland | HPRA / hpra.ie |
| `DK` | 🇩🇰 Denmark | DKMA / produktresume.dk |
| `NO` | 🇳🇴 Norway | NoMA / felleskatalogen.no |
| `FI` | 🇫🇮 Finland | Fimea / laakeinfo.fi |
| `SE` | 🇸🇪 Sweden | MPA / fass.se |

### Oost-Europa

| Code | Country | Medicine registry source |
|------|---------|--------------------------|
| `PL` | 🇵🇱 Poland | URPL / rejestry.ezdrowie.gov.pl |
| `CZ` | 🇨🇿 Czech Republic | SÚKL / opendata.sukl.cz |
| `SK` | 🇸🇰 Slovakia | ŠÚKL / sukl.sk |
| `HU` | 🇭🇺 Hungary | OGYÉI / ogyei.gov.hu |
| `RO` | 🇷🇴 Romania | ANMDMR / anm.ro |
| `HR` | 🇭🇷 Croatia | HALMED / halmed.hr |
| `SI` | 🇸🇮 Slovenia | JAZMP / jazmp.si |
| `GR` | 🇬🇷 Greece | EOF / eof.gr |
| `PT` | 🇵🇹 Portugal | INFARMED / infarmed.pt |

### Noord-Amerika

| Code | Country | Medicine registry source |
|------|---------|--------------------------|
| `US` | 🇺🇸 United States | FDA NDC / open.fda.gov |
| `CA` | 🇨🇦 Canada | Health Canada DPD |

Each database contains medicines sourced from the official national registry, supplemented with EMA (European Medicines Agency) centrally authorised products. If an unknown country code is set, the app falls back to `BE`.

## Project structure

```
apohouze/
├── server.js                        # Express API (auth, validation, SQLite sessions)
├── lib/
│   └── totp.js                      # TOTP (RFC 6238), no dependencies
├── db/
│   └── database.js                  # SQLite + country loader (reads data/countries/*.js)
├── data/
│   └── countries/
│       ├── be.js                    # Belgium
│       ├── nl.js, de.js, fr.js ...  # West-Europa
│       ├── gb.js, ie.js, dk.js ...  # Noord-Europa
│       ├── cz.js, sk.js, hu.js ...  # Oost-Europa
│       ├── ro.js, hr.js, si.js ...  # Oost-Europa
│       ├── gr.js, lu.js             # Oost-Europa / West-Europa
│       └── us.js, ca.js             # Noord-Amerika
├── public/
│   ├── index.html                   # PWA frontend
│   ├── manifest.json
│   └── sw.js
├── fetch_eu_medicines.py            # Fetcher: AT CH DK ES FI IE NO PL PT SE
├── fetch_new_eu_medicines.py        # Fetcher: CZ SK HR SI HU RO GR LU
├── fetch_de_medicines.py            # Fetcher: DE
├── fetch_gb_medicines.py            # Fetcher: GB
├── fetch_fr_medicines.py            # Fetcher: FR
├── fetch_it_medicines.py            # Fetcher: IT
├── fetch_us_medicines.py            # Fetcher: US
├── fetch_ca_medicines.py            # Fetcher: CA
├── update.js                        # Central updater (Node.js)
├── Dockerfile
├── docker-compose.yml
└── package.json
```

## Updating the medicine database

Run locally or via GitHub Actions:

```bash
# Eén land updaten
node update.js be

# Meerdere landen tegelijk
node update.js cz sk hr si hu ro gr lu

# Alle landen
node update.js be nl de gb fr it us ca at ch dk es fi ie no pl pt se cz sk hr si hu ro gr lu

# Dry-run (geen bestanden gewijzigd)
node update.js cz --dry-run
```

The updater fetches data from the official source, deduplicates by name, and appends new entries to the country file.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RESET_ACCOUNT` | — | `true` removes the account + 2FA on start so you can set them up again (remove afterwards) |
| `SESSION_SECRET` | random | If empty/default, a random secret is generated once and stored in the database |
| `DB_PATH` | `/data/apohouze.db` | Path to the SQLite database |
| `PORT` | `3000` | Internal port |
| `COUNTRY` | `BE` | Country code (see table above); unknown codes fall back to `BE` |
| `TRUST_PROXY` | — | Set to `1` when running behind an HTTPS reverse proxy |
| `COOKIE_SECURE` | `auto` | `true` / `false` to force the session cookie flag |
| `TZ` | `Europe/Brussels` | Time zone used for "expires within 30 days" calculations |

## Security notes

- Sessions are stored in SQLite, so you stay logged in across container restarts.
- Account is created in the app (setup code from the log); two-step verification (TOTP, RFC 6238) is mandatory. Codes can't be replayed, and 5 wrong codes end the login attempt.
- Login is rate limited (10 failed attempts per 15 minutes per IP) and uses constant-time comparison.
- Session cookies are `HttpOnly` and `SameSite=Lax`; the cookie is `Secure` automatically over HTTPS (set `TRUST_PROXY=1` behind a proxy).
- The container runs as an unprivileged user. If your data folder was created by an older version (as root), run once: `chown -R 1026:100 /volume1/docker/apohouze` (adjust to your UID:GID) or set `user: "0:0"` in the compose file.
- Health endpoint for monitoring: `GET /healthz`.

## Upgrading from v2.0 / v2.1

`APP_USERNAME`, `APP_PASSWORD` and `APP_PASSWORD_HASH` are no longer used — remove them from the compose file. After the update the app asks you to create an account (setup code in the log) and enrol two-step verification; existing sessions are signed out. Nothing else to migrate: the database is upgraded automatically on first start. The medicine list is re-imported once (a new accent-insensitive search column is added). Your inventory is untouched. After updating, hard-refresh once so the new service worker takes over.

## CI/CD

- **Update Medicines Data** runs every Sunday, commits refreshed country files and then triggers
- **Build & Push Docker Image**, which publishes `ghcr.io/bigbuud/apohouze:latest`. (Previously the data commit never rebuilt the image.)

## License

MIT
