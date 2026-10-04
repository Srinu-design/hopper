# Grafana alerting provisioning

Empty on purpose. Hopper's alerts are Prometheus rules in `deploy/prometheus/alerts.yml`,
unit-tested with `promtool test rules` (`make alerts`), and Grafana shows them through the
Prometheus data source. This folder exists only because Grafana logs an error at startup when
`provisioning/alerting` is missing.
