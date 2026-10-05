# A release broken on purpose: /readyz always answers 503, as a bad commit would make it.
# Used by deploy/rehearse.sh and the rollback drill (.github/workflows/drill.yml) to prove that a
# release failing its health checks is rolled back automatically. Never deploy it otherwise.
#   docker build -f deploy/broken/readyz.Dockerfile --build-arg BASE=<good image> -t <tag> deploy/broken
ARG BASE
FROM ${BASE}
USER root
RUN f="$(python -c 'import hopper.api.health as m; print(m.__file__)')" \
 && sed -i 's/    if results\["postgres"\] != "ok":/    if True:  # BROKEN ON PURPOSE/' "$f" \
 && grep -q "BROKEN ON PURPOSE" "$f" \
 && rm -rf "$(dirname "$f")/__pycache__"
USER app
