# Deploy: Render vs monorepo (submodule)

## Var byggs Docker-imaget?

**Render** kör `docker build` mot repot du kopplar i Render — normalt **[full_scraper](https://github.com/Jakeminator123/full_scraper)** på branch **`main`** (eller den branch du valt i Render).

- **Ingen** Docker-build körs från roten av `alla-skrapningsprojekt` för denna tjänst.
- Källkoden som går in i containern är allt som finns i **full_scraper**-repot vid den commit Render drar.

## Monorepo med submodule

I **alla-skrapningsprojekt** ligger `services/full_scraper` som en **git submodule** som pekar på samma GitHub-repo. Det är **två separata git-huvuden**:

| Var | Vad spårar `main` |
|-----|-------------------|
| **full_scraper** (eget repo) | Koden Render bygger |
| **alla-skrapningsprojekt** | En **commit-hash** för submodule-mappen |

Efter du pushat ny kod till `full_scraper`:

```bash
cd path/to/alla_skrapningsprojekt
git add services/full_scraper
git commit -m "chore: bump full_scraper submodule"
git push
```

Annars visar monorepot en **äldre** submodule-commit än `origin/main` i submodule-repot — Cursor/statusraden kan visa `main*` i parent medan submodule redan är checkad på ny commit (`+` i `git submodule status`).

## Checklista

1. Push **`full_scraper`** → Render auto-deploy (om aktiverat).
2. Vill du att monorepot ska spegla samma revision → **bump submodule** + push parent.

## Dashboard

Öppna **`https://<din-render-service>/`** (root). Ingen separat Vercel-frontend krävs för denna setup.
