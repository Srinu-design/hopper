# A release broken on purpose: every worker exits at start, while the API and /readyz stay
# healthy. Only the smoke test's real job can catch this one. Used by deploy/rehearse.sh and the
# rollback drill (.github/workflows/drill.yml). Never deploy it otherwise.
#   docker build -f deploy/broken/worker.Dockerfile --build-arg BASE=<good image> -t <tag> deploy/broken
ARG BASE
FROM ${BASE}
USER root
RUN f="$(python -c 'import hopper.worker.__main__ as m; print(m.__file__)')" \
 && sed -i 's/^    asyncio.run(main())/    raise SystemExit("BROKEN ON PURPOSE: this worker exits at start")/' "$f" \
 && grep -q "BROKEN ON PURPOSE" "$f" \
 && rm -rf "$(dirname "$f")/__pycache__"
USER app
