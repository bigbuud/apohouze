# apoHouze 💊

apoHouze — home medicine app — multi-country PWA with Docker support.

## Quick start

1. Download `docker-compose.yml`
2. Set your country code in `COUNTRY:` (see list below)
3. Run `docker compose up -d`
4. Open `http://your-server:3525`

```yaml
environment:
  APP_USERNAME: admin
  APP_PASSWORD: yourpassword
  SESSION_SECRET: change-this-to-a-long-random-secret
  DB_PATH: /data/apohouze.db
  COUNTRY: BE          # ← change this
```

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
├── server.js                        # Express API
├── db/
│   └── database.js                  # SQLite + dynamic country loader
├── data/
│   └── countries/
│       ├── be.js                    # Belgium
│       ├── nl.js, de.js, fr.js ...  # West-Europa
│       ├── gb.js, ie.js, dk.js ...  # Noord-Europa
│       ├── cz.js, sk.js, hu.js ...  # Oost-Europa  ← nieuw
│       ├── ro.js, hr.js, si.js ...  # Oost-Europa  ← nieuw
│       ├── gr.js, lu.js             # Oost-Europa / West-Europa ← nieuw
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
| `APP_USERNAME` | `admin` | Login username |
| `APP_PASSWORD` | `apohouze123` | Login password |
| `SESSION_SECRET` | — | Change to a long random string |
| `DB_PATH` | `/data/apohouze.db` | Path to SQLite database |
| `PORT` | `3000` | Internal port |
| `COUNTRY` | `BE` | Country code (see table above) |

## HTTPS / reverse proxy

If running behind nginx, Traefik or Nginx Proxy Manager with HTTPS, set `secure: true` in `server.js` and add `app.set('trust proxy', 1)`.

## License

MIT
