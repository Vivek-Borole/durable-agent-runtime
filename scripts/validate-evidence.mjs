import { readFile } from "node:fs/promises";

async function readJson(path) {
  return JSON.parse(await readFile(new URL(`../${path}`, import.meta.url), "utf8"));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertMachine(machine, label) {
  for (const key of ["os", "arch", "cpus", "memoryBytes", "goVersion", "nodeVersion", "dockerVersion"]) {
    assert(machine?.[key] !== undefined && machine[key] !== "", `${label}: missing machine.${key}`);
  }
}

const fault = await readJson("docs/evidence/effect-fault-report.json");
assert(fault.attempts === 100000, "fault report must contain 100,000 attempts");
assert(fault.committedEffects === 1 && fault.passed === true, "fault report must prove one committed effect");
assert(Array.isArray(fault.failureConditions) && fault.failureConditions.length > 0, "fault report needs failure conditions");
assertMachine(fault.machine, "fault report");

const benchmark = await readJson("docs/evidence/scheduling-benchmark-report.json");
assert(benchmark.residentActiveMockRuns === 1000, "benchmark must include 1,000 resident mock workflows");
assert(benchmark.backlogRuns === 10000, "benchmark must include a 10,000-run backlog");
assert(benchmark.succeededRuns === 10000 && benchmark.passP95Under500Millis === true, "benchmark must pass all measured runs and p95 gate");
assert(benchmark.p95Millis <= 500, "benchmark p95 exceeds 500 ms");
assert(Array.isArray(benchmark.failureConditions) && benchmark.failureConditions.length > 0, "benchmark needs failure conditions");
assertMachine(benchmark.machine, "benchmark report");

const recovery = await readJson("docs/evidence/lease-recovery-report.json");
assert(recovery.passed === true, "lease recovery evidence did not pass");
assert(recovery.attempts === 2 && recovery.committedEffects === 1 && recovery.finalState === "succeeded", "lease recovery outcome is incomplete");

console.log("release evidence schema is valid");
