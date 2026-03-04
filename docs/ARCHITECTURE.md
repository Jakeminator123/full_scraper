# Arkitektur — full_scraper

## Triangeln: tre sajter, inget lösenord

Tre publika sajter korsrefereras för att gå från **enbart personnummer** till
**fullständig personinformation** — utan inloggning, utan BankID.

```
                    ┌──────────────────────┐
                    │    PERSONNUMMER       │
                    │    19860528-0299      │
                    └──────────┬───────────┘
                               │
             ┌─────────────────┤
             │                 │
             ▼                 │
  ┌─────────────────────┐     │
  │   BILUPPGIFTER.SE   │     │
  │                     │     │
  │  base64(pnr) → URL  │     │
  │  Ger: namn           │     │
  └──────────┬──────────┘     │
             │                 │
             │   namn + pnr    │
             │                 │
             ▼                 │
  ┌─────────────────────┐     │
  │     RATSIT.SE       │     │
  │                     │◄────┘  (fallback: pnr + könsfilter)
  │  Sök-API → 1 träff   │
  │  Ger: namn, adress,  │
  │  ålder, kön, gift,   │
  │  bolag, koordinater  │
  └──────────┬──────────┘
             │
             │   namn
             │
             ▼
  ┌─────────────────────┐
  │    MERINFO.SE       │
  │                     │
  │  Sök på namn+stad   │
  │  Ger: medboende,    │
  │  fordon på adressen,│
  │  lönestatistik,     │
  │  kronofogde-snitt   │
  └─────────────────────┘
```

---

## Så knäcktes koden

### Problemet

Ratsit maskerar personnumrets sista 4 siffror: `19860528-XXXX`.
Sökning med personnummer utan inloggning ignorerar individnumret helt
och returnerar alla ~400 personer födda samma dag.

### Upptäckten: base64 i biluppgifter-URL

Varje profilsida på Ratsit anropar ett internt API:

```
GET https://www.ratsit.se/person/biluppgifter/{person-hash}
```

Svaret innehåller:

```json
{
  "subjectUri": "https://biluppgifter.se/brukare/MTk4NjA1MjgwMjk5",
  "subjectNumberOfVehicles": 0,
  "vehiclesOnAddress": [...]
}
```

Strängen `MTk4NjA1MjgwMjk5` är standard **base64** — ingen kryptering:

```
base64("198605280299") = "MTk4NjA1MjgwMjk5"
Buffer.from("MTk4NjA1MjgwMjk5", "base64").toString() = "198605280299"
```

Det fullständiga personnumret ligger i klartext, bara omkodat.

### Den omvända vägen: personnummer → namn

Om base64(personnummer) finns i URL:en **till** biluppgifter.se, kan vi
bygga URL:en **från** personnumret:

```
personnummer:  19860528-0299
utan streck:   198605280299
base64:        MTk4NjA1MjgwMjk5
URL:           https://biluppgifter.se/brukare/MTk4NjA1MjgwMjk5
```

Biluppgifter.se renderar **server-side** (htmx, ingen JS krävs) och visar:

> *"Emelie Lundqvist, en privatperson som är 39 år..."*
> *"Visa Emelie Lundqvist på Ratsit"*

### Kombinerat flöde

Med namnet från biluppgifter söker vi `"Emelie Lundqvist 19860528-0109"`
på Ratsit — och får **exakt 1 träff** med fullständig data.

Total tid: **3–6 sekunder**, 100% headless.

---

## Datakällor per sajt

### Biluppgifter.se (steg 1)

| Fält | Tillgängligt |
|------|-------------|
| Namn (för+efternamn) | Ja |
| Ålder | Ja |
| Stad | Ja |
| Adress | Ja |
| Fordon (personen) | Ja |
| Fordon (adressen) | Ja |
| **Personnummer** | **Nej** — PNR måste redan vara känt |

Teknik: Server-side rendering (htmx). Innehåll klart vid `domcontentloaded`.
Ingen cookie-banner, inga API-anrop att intercepta.

**Viktigt:** Fordonssidor (`/fordon/{REGNR}/`) visar **aldrig** personnummer
eller ägardata utan inloggning. Även fordon med `vehicleId` i URL:en kräver
BankID för att se vem som äger bilen. Fas 1 ger därmed bara fordonsdata
(regnr, modell, färg, etc), aldrig PNR.

### Ratsit.se (steg 2)

| Fält | Tillgängligt |
|------|-------------|
| Fullständigt namn | Ja |
| Tilltalsnamn | Ja |
| Ålder | Ja |
| Gatuadress + postnr | Ja |
| Stad | Ja |
| Kön | Ja |
| Civilstånd | Ja |
| Bolagsengagemang | Ja |
| GPS-koordinater | Ja |
| Personnummer (fullt) | Via biluppgifter-API (subjectUri) |
| Lön | Kräver betalning |
| Kreditupplysning | Kräver BankID |

Teknik: SPA med internt POST-API (`/api/search/combined`). Interceptas
via Playwright response-events.

### Merinfo.se (steg 3)

| Fält | Tillgängligt |
|------|-------------|
| Namn, ålder, adress | Ja (samma som Ratsit) |
| Medboende (namn+ålder) | Ja |
| Fordon på adressen | Ja |
| Lönestatistik (gatusnitt) | Ja |
| Kronofogde-statistik | Ja (kommun-snitt) |
| Personnummer (fullt) | Nej (XXXX, kräver login) |
| Telefonnummer | Kräver betalning |

Teknik: SPA med REST-API (`/api/v1/search/results`, `/api/v1/people/{uuid}/*`).
Cookie-consent krävs. Personnummersökning fungerar ej.

---

## Luhn-algoritmen — hur kontrollsiffran räknas ut

Varje giltigt svenskt personnummer avslutas med en **kontrollsiffra** som är
matematiskt beroende av de nio föregående siffrorna. Det är Luhn-algoritmen
(ISO 7812), samma algoritm som används för kreditkortsnummer.

Syftet i systemet: vi kan **generera alla giltiga PNR-kombinationer lokalt**
utan att fråga någon server — och sedan filtrera bort ogiltiga på under en
mikrosekund per PNR.

### Steg för steg med `19860528-029?`

De nio siffrorna som matas in i algoritmen är: `8 6 0 5 2 8 0 2 9`
(årtusende+sekel utelämnas, dvs `86` inte `1986`).

```
Position (0-indexerad):  0  1  2  3  4  5  6  7  8
Siffra:                  8  6  0  5  2  8  0  2  9

Steg 1 — dubblera varje siffra på jämn position (0, 2, 4, 6, 8):
  pos 0: 8 × 2 = 16  → 16 > 9 → 16 − 9 = 7
  pos 1: 6           →                      6
  pos 2: 0 × 2 =  0  →                      0
  pos 3: 5           →                      5
  pos 4: 2 × 2 =  4  →                      4
  pos 5: 8           →                      8
  pos 6: 0 × 2 =  0  →                      0
  pos 7: 2           →                      2
  pos 8: 9 × 2 = 18  → 18 > 9 → 18 − 9 = 9

Steg 2 — summera:  7 + 6 + 0 + 5 + 4 + 8 + 0 + 2 + 9 = 41

Steg 3 — kontrollsiffra:  (10 − (41 % 10)) % 10 = (10 − 1) % 10 = 9
```

Kontrollsiffran är **9**. Det fullständiga PNR:et är `19860528-0299`.

Implementationen i `scraper.py`:

```python
def _luhn_check(nine_digits: str) -> int:
    total = 0
    for i, ch in enumerate(nine_digits):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return (10 - (total % 10)) % 10
```

### Hur individnumret (nnn) avslöjar kön

Löpnumret `nnn` (029 i exemplet) har en inbyggd könskodning:
- **Udda nnn** (001, 003, 005 … 999) = **man**
- **Jämnt nnn** (002, 004, 006 … 998) = **kvinna**

Det ger 500 möjliga individnummer per kön per dag.

### Varför detta möjliggör brute-force

Med Luhn-algoritmen behöver vi aldrig gissa kontrollsiffran. För varje
kombination av `(år, månad, dag, individ)` beräknas den deterministiskt:

```
för år 1986, mån 1..12, dag 1..31, individ 1..999:
    pnr = make_pnr(år, mån, dag, individ)   # None om ogiltigt datum
    if pnr:
        fetch biluppgifter.se/brukare/{base64(pnr)}/
```

Antalet kombinationer för åren 1940–2005:

```
66 år × 12 mån × 31 dagar × 999 individ ≈ 24,5 miljoner möjliga PNR
```

Av dessa ger `make_pnr()` `None` för ogiltiga datum (t.ex. 31 juni,
29 februari icke-skottår) — det eliminerar ~8% automatiskt.

---

## full_scraper — Docker-arkitektur

`full_scraper/` är en självständig Docker-tjänst som kör på Render med
persistent disk. Tre faser körs i sekvens/parallell för maximal täckning.

### De tre faserna

```
Fas 0 — Ratsit-harvesting (smart, ~3–5 dagar, 100% träffgrad)  ← NY
  Sök Ratsit med varje födelsedag (YYYYMMDD) + könsfilter
  24 000 dagar × 2 kön = 48 000 sökningar
  Varje sökning: intercepta /api/search/combined → max 30 träffar
  Per träff: besök profilsida → intercepta /person/biluppgifter/ →
    extrahera base64(PNR) från subjectUri → avkoda till PNR
  PNR → biluppgifter.se/brukare/{b64}/ → spara i people-tabell
  → ~1,4M personer med fullständig data

Fas 1 — Fordon (snabb, ~1–2 dagar)
  biluppgifter.se /fordon/AAA/ → /fordon/ZZZ/
  17 576 prefix × ~7 sidor/prefix × 100 fordon/sida
  → ~11,4M fordon sparas i vehicles-tabell
  Inga personnummer — ägardata kräver inloggning

Fas 2 — PNR-enumeration (täcker resten, ~10–14 dagar efter Fas 0)
  Generera alla ~24,5M giltiga PNR (1940–2005, dag 1–31)
  → base64 → biluppgifter.se/brukare/{b64}/
  → parsas → sparas i people-tabell
  Träffrate: ~35–45% (resten har aldrig funnits eller är avregistrerade)
  Fas 0-PNR är redan i DB → existing_pnrs() hoppar automatiskt över dem
```

Alla tre faser körs **parallellt** i `_run()`:

```python
# Fas 0 i bakgrundstråd (konservativ rate mot Ratsit, 1 Chromium)
phase0_thread = threading.Thread(target=_run_phase0, daemon=True)
phase0_thread.start()

# Fas 1 i bakgrundstråd (5 Chromium mot biluppgifter fordonslistor)
phase1_thread = threading.Thread(target=_run_phase1, daemon=True)
phase1_thread.start()

# Fas 2 i huvudtråden (5 Chromium mot biluppgifter brukarsidor)
# existing_pnrs() hoppar automatiskt over PNR som Fas 0 redan hittat
_run_phase2(start_year, end_year, target)
```

### Phase 0 i detalj

Ratsit returnerar ~400 personer per datum, men visar max 30 utan inloggning
(3 sidor × 10 resultat/sida). Kön-splittering (man/kvinna) ger 60 unika per datum.

PNR-utvinning via Ratsit-profilsidor:
- Ratsit maskerar PNR som `19860528-XXXX` i sökresultaten
- Men varje profilsida anropar `GET /person/biluppgifter/{hash}`
- Svaret innehåller `{"subjectUri": "https://biluppgifter.se/brukare/MTk4..."}
- base64-avkodning: `MTk4NjA1MjgwMjk5` → `198605280299` → `19860528-0299`
- Verifierat: 30/30 PNR löste sig utan fel vid test med datum 1986-05-28

Filerna som implementerar Fas 0:
- `ratsit_helper.js` — Playwright-skript, tar `{date, gender}` via stdin, returnerar lista med PNR+metadata
- `_run_phase0()` i `scraper.py` — itererar datum, anropar `ratsit_helper.js`, hämtar biluppgifter-sidor, sparar i DB

### Konfiguration (miljövariabler)

```
PAGE_PAUSE=0.3           sekunder mellan requests per worker (Fas 1 + 2)
PHASE0_PAUSE=1.0         sekunder mellan requests mot Ratsit (Fas 0)
BATCH_SIZE=30            PNR per fetch_helper.js-anrop
PARALLEL_WORKERS=5       parallella Chromium-instanser per fas (Fas 1 + 2)
START_YEAR=1940          första födelseåret att enumerera
END_YEAR=2005            sista födelseåret
AUTO_RESUME=true         återuppta automatiskt vid omstart
```

### Hastighetstaket och tidsuppskattning

Alla tre faser körs parallellt. Den totala tiden bestäms av den
långsammaste fasen (Fas 2), men Fas 0 minskar Fas 2:s arbete löpande.

```
Fas 0: ~160s per datum × 2 kön ≈ 5 min/datum (sekventiell, 1 Chromium)
       Körs parallellt i bakgrunden, slutar inte förrän alla datum testats.
       ~1.4M PNR sparas → Phase 2 hoppar automatiskt över dem.

Fas 1: ~2 dagar (fordon, 5 workers parallellt)

Fas 2: 5 workers × (1 / 0.3s paus) ≈ 16 URL/s
       24.5M PNR totalt, varav ~1.4M hoppas över (Fas 0-fynd)
       Sparar ~1.4M × 0.06s/URL ≈ 23 timmar (≈1 dag) jämfört med utan Fas 0.
       Total Fas 2-tid: ~17 dagar (ned från ~18)
```

**Fas 0-flaskhalsen:** Ratsit tillåter inte parallella sökningar utan risk för blockering.
`PHASE0_PAUSE=1.0s` är konservativt; i praktiken tar varje profil-besök 1–3s.
Fas 0 använder bara 1 Chromium-instans → låg RAM-påverkan (~150 MB extra).

**Fas 1+2-flaskhalsen:** Cloudflare på biluppgifter.se.
Testat safe-område: 3–8 workers med 0.3s paus.

**Netto-tidsvinst:** ~1 dag snabbare totalt. Fas 0 adderar däremot ~1.4M
verifierade personer med 100% träffgrad (via Ratsit → PNR → biluppgifter),
jämfört med brute-force:ens ~35-45% träffgrad.

### Checkpointing och resume

`job_state`-tabellen i SQLite sparar exakt position för alla tre faser:

| Kolumn | Fas |
|---|---|
| `phase0_date` | Senast behandlade datum i Fas 0 (YYYY-MM-DD) |
| `phase0_found` | Antal personer hittade i Fas 0 |
| `phase1_prefix` | Senast behandlade prefix i Fas 1 |
| `phase1_prefixes_done` / `phase1_vehicles` | Fas 1 framsteg |
| `current_year/month/day/individ` | Position i Fas 2 |
| `total_tested` / `total_found` | Fas 2 räknare |

Vid omstart (deploy, OOM-crash, manuell restart):
1. Databasen ligger på persistent disk (`/var/data/people.db`)
2. `AUTO_RESUME=true` startar om från senaste checkpoint för alla tre faser
3. `INSERT OR IGNORE` förhindrar dubbletter
4. `existing_pnrs()` hoppar över redan kända PNR innan fetch (sparar HTTP)

Enda sättet att börja om helt: `POST /job/reset` (raderar alla tabeller).

### API — kontrollera och hoppa över faser

```
POST /job/start?skip_phase0=true   hoppa över Fas 0 (Ratsit-harvesting)
POST /job/start?skip_phase1=true   hoppa över Fas 1 (fordon-enumeration)
POST /job/start?skip_phase0=true&skip_phase1=true   bara Fas 2
```

### Optimeringar implementerade

| Optimering | Effekt |
|---|---|
| **Fas 0: Ratsit-harvesting** | ~1,4M personer med 100% träffgrad, ~3–5 dagar |
| Dag 1–31 (inte bara 1–28) | +10% av befolkningen som föddes 29–31:a |
| BATCH_SIZE=30 (var 15) | Amorterar Chromium-startoverhead bättre |
| Blockera bilder/CSS/fonts | ~60% mindre bandbredd per request |
| `existing_pnrs()` DB-check | Fas 0-PNR hoppas över i Fas 2 automatiskt |
| Ny browser context per URL | Undviker Cloudflare navigeringsmönster-blockering |

---

## Varför Fas 1 inte kan ersätta Fas 2

En vanlig fråga: om Fas 1 hämtar ~11M fordon, kan man inte få
personnummer från fordonsägarna och skippa PNR-brute-force?

**Nej.** Verifierat 2026-03-04:

- Fordonssidor (`/fordon/{REGNR}/` och `/fordon/{REGNR}/{vehicleId}/`)
  visar **aldrig** personnummer, namn eller adress utan inloggning
- Även med `vehicleId` i URL:en visas bara:
  *"Ägare: Endast inloggade medlemmar kan se ägarinformation"*
- `vehicleId` är bara en intern UUID — den låser inte upp ägardata

Fas 1 ger fordonsdata (regnr, modell, år, status). Fas 2 är **enda
publika vägen** till personnummer och personuppgifter.

~60% av Sveriges befolkning äger dessutom inget fordon, så Fas 2
behövs oavsett för att täcka hela registret.

---

## Säkerhet och begränsningar

- **Sekretessmarkerade personer** (~1%) syns inte på någon sajt
- **Utan inloggning** visar Ratsit max 30 sökresultat (3 sidor)
- **Rate limiting** testat med 20 snabba anrop — inga blockeringar
- **Cloudflare** sitter framför alla tre sajter men triggas inte vid normal volym
- **Base64 är inte kryptering** — vem som helst kan avkoda `MTk4NjA1MjgwMjk5`
  till `198605280299` på en sekund
