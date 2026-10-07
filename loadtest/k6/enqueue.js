// Enqueue load for Hopper: POST /v1/jobs at a constant arrival rate (the build guide's k6
// script). loadtest/bench.py drives it for scenarios A, C and D; it also runs on its own:
//
//   docker run --rm --network host -v "$PWD/loadtest:/work" -w /work \
//     -e BASE=http://127.0.0.1:8000 -e KEY=hop_live_... -e RATE=250 grafana/k6:2.3.0 \
//     run k6/enqueue.js
//
// RATE      requests per second (default 250)
// DURATION  how long (default 2m)
// MIX       sleep: every job sleeps 50 ms (A, D); steady: the same plus 5% flaky jobs (C)
// SUMMARY   where to write the full end-of-test summary as JSON (optional)
import http from 'k6/http';
import { check } from 'k6';

const RATE = Number(__ENV.RATE || 250);
const MIX = __ENV.MIX || 'sleep';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    enqueue: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '2m',
      preAllocatedVUs: Number(__ENV.VUS || 200),
      maxVUs: Number(__ENV.MAX_VUS || 2000),
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<100'] },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

const params = {
  headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${__ENV.KEY}` },
  tags: { name: 'POST /v1/jobs' },
};
const SLEEP = JSON.stringify({ task: 'sleep', payload: { ms: 50 } });
const FLAKY = JSON.stringify({ task: 'flaky', payload: { p: 0.5, ms: 50 } });

export default function () {
  const body = MIX === 'steady' && Math.random() < 0.05 ? FLAKY : SLEEP;
  const res = http.post(`${__ENV.BASE}/v1/jobs`, body, params);
  check(res, { created: (r) => r.status === 201 });
}

export function handleSummary(data) {
  const m = data.metrics;
  const d = m.http_req_duration.values;
  const line =
    `rate ${RATE}/s: ${m.http_reqs.values.count} requests, ` +
    `${m.http_reqs.values.rate.toFixed(1)}/s achieved, ` +
    `p50 ${d.med.toFixed(1)} ms, p95 ${d['p(95)'].toFixed(1)} ms, ` +
    `p99 ${d['p(99)'].toFixed(1)} ms, failed ${(m.http_req_failed.values.rate * 100).toFixed(2)}%, ` +
    `dropped ${m.dropped_iterations ? m.dropped_iterations.values.count : 0}\n`;
  const out = { stdout: line };
  if (__ENV.SUMMARY) out[__ENV.SUMMARY] = JSON.stringify(data, null, 1);
  return out;
}
