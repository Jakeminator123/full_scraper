# Aviseringar och webhooks

Det finns **ingen inbyggd** “varsel-webhook” i själva scraper-koden som skickar till Slack vid varje fel. Istället använder du plattformarnas notifieringar och valfria externa health checks.

## Render (API / Docker)

- **Deploy & drift:** [Render Notifications](https://render.com/docs/notifications) — koppla e-post eller Slack när deploy misslyckas eller instansen går ner.
- **Deploy webhook:** I tjänstens inställningar kan du ofta lägga en **outgoing webhook** vid lyckad/misslyckad deploy (beroende på Render-plan och UI).
- **OOM / krascher:** Övervaka **Events**-loggen; kombinera med notifieringar om du vill ha push vid instansfel.

## Uptime / “är API levande?”

- Pinga **`GET /health`** (ingen auth) eller **`GET /diag`** (ingen auth) med t.ex. [healthchecks.io](https://healthchecks.io), UptimeRobot eller liknande.
- Om du vill larm vid **tom databas** eller **inaktiv scraper** behöver du antingen ett eget litet script som tolkar `/diag`, eller utöka appen med ett dedikerat endpoint — det finns inte standardiserat i repot idag.

## Säkerhet

- **`API_KEY`** ska bara finnas som miljövariabel på servern (Render). Webbläsaren lagrar den **endast i `sessionStorage`** när du använder inbyggda dashboarden på samma host — dela inte länkar med inbäddad nyckel.
