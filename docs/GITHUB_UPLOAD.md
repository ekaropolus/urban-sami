# GitHub Upload

This repository is already initialized locally and has an initial commit on branch `main`.

## Current local state

- local branch: `main`
- initial commit already created
- heavyweight generated folders are excluded by `.gitignore`

## What to do on GitHub

1. Go to <https://github.com/new>.
2. Create an empty repository.
3. Recommended name: `urban-sami`
4. Do **not** initialize it with:
   - `README`
   - `.gitignore`
   - license

If you want a public release, choose the visibility now. If you still need to decide licensing, private first is safer.

## Add the remote and push

From the repository root:

```bash
cd /home/hadox/cmd-center/platforms/research/urban-sami
git remote add origin git@github.com:<YOUR_USER_OR_ORG>/urban-sami.git
git push -u origin main
```

If you prefer HTTPS:

```bash
cd /home/hadox/cmd-center/platforms/research/urban-sami
git remote add origin https://github.com/<YOUR_USER_OR_ORG>/urban-sami.git
git push -u origin main
```

## Check the remote

```bash
git remote -v
git branch
git log --oneline -n 3
```

## Recommended first GitHub checks

After the push:

1. open the repo homepage
2. verify `README.md` renders well
3. open `PROJECT_MAP.md`
4. open `docs/monograph-script-crosswalk.csv`
5. open `manuscript/final-multiscale-monograph-2026-04-25/main.pdf`

## Important note on artifacts

This repository intentionally does **not** push the heavy local artifact folders:

- `data/`
- `reports/`
- `logs/`
- `dist/`

Those are meant to be regenerated locally from:

- the scripts in `scripts/`
- the SQL/bootstrap assets in `sql/`
- the persistent database workflow described in `docs/REPRODUCIBILITY.md`

## If you later want a heavier archive

You have two sane options:

1. keep this repository as the light, reviewable code-and-monograph repo
2. publish large outputs separately:
   - GitHub Releases assets
   - a second archive repo
   - an object store or DOI archive
