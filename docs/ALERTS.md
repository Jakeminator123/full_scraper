# Aviseringar och webhooks

Det finns **ingen inbyggd** “varsel-webhook” i själva scraper-koden som skickar till Slack vid varje fel. Istället använder du plattformarnas notifieringar och valfria externa health checks.

## Render (API / Docker)

- **Deploy & drift:** [Render Notifications](https://render.com/docs/notifications) — koppla e-post eller Slack när deploy misslyckas eller instansen går ner.
- **Deploy webhook:** I tjänstens inställningar kan du ofta lägga en **outgoing webhook** vid lyckad/misslyckad deploy (beroende på Render-plan och UI).
- **OOM / krascher:** Övervaka **Events**-loggen; kombinera med notifieringar om du vill ha push vid instansfel.

## Vercel (dashboard)

- **Deploy:** [Vercel → Project → Settings → Git → Deploy Hooks](https://vercel.com/docs/deploy-hooks) eller integrerade notifieringar vid failed production deploy.

## Uptime / “är API levande?”

- Pinga **`GET /health`** (ingen auth) eller **`GET /diag`** (ingen auth) med t.ex. [healthchecks.io](https://healthchecks.io), UptimeRobot eller liknande.
- Om du vill larm vid **tom databas** eller **inaktiv scraper** behöver du antingen ett eget litet script som tolkar `/diag`, eller utöka appen med ett dedikerat endpoint — det finns inte standardiserat i repot idag.

## Säkerhet

- Lägg **aldrig** riktiga API-nycklar eller `DASHBOARD_PASSWORD` i frontend-repot. På Vercel ska `SCRAPER_API_KEY` och `DASHBOARD_PASSWORD` bara finnas som **environment variables**.
