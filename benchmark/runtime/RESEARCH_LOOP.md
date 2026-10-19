# Coordinated research-loop treatment

For a **fresh** EdgeBench `heartbeat-explore` attempt with native public feedback
and a custom Responses-compatible endpoint, `--research-loop` enables one
coordinated treatment. Omission preserves the previous treatment. The runner
rejects adding it to an older checkpoint. Runtime, worker and export receipts
retain the flag; do not compare mixed treatments as identical configurations.
Roll back by omitting the flag on a new attempt. Retained findings containing
the new optional observation require a supporting reader; do not rewrite old
history or claim that an older strict event reader understands the new field.
The treatment selects the existing `evidence_conditioned_v1` replan strategy;
an explicitly conflicting strategy is rejected. This is a combined treatment,
not an isolated estimate of the contribution of any one component.

The entry creates an initial-candidate Todo and a separate deferred research
Todo using existing `todo_done`/successor semantics. Finishing the candidate
releases real optimization work; it cannot by itself close that work. The
original task remains the Goal objective. Research retains the best candidate,
meaningful negative results and reasoned closeout against the original objective.
User Stop, the original budget and genuine blockers remain authoritative. There
is no requirement to fill time or change the native scoring/submission rules.
Preparation records this scope through the ordinary Agent Vision writer as
`surface_only`, without claiming research progress or spending a work Turn.
Existing prepared research work is not reopened or its later Vision overwritten.

The worker reuses the live Host launch owner before **each** provider request,
including requests inside one long native wake. Current wait/Stop/refusal means
no upstream send and interruption of the owned native process; the original
scheduler handles wait/recovery. Checkpoint continuations retain the held Turn.
Unreadable authority fails closed. Already-forwarded requests cannot be
retroactively cancelled, and absent canonical waits are not inferred from prose.

Private `provider-requests.jsonl` correlates wake, LoopX Turn, native
session/thread/Turn when supplied, client request, gateway request and returned
upstream request identities. A dispatch without a finished record is unknown;
`forwarded=null` preserves ambiguity during a failed send. Missing identifiers
remain null. The journal excludes bodies, credentials and upstream endpoints.
Qualify the pinned client at its request boundary. Admission adds control cost;
include it in subsequent matched comparisons instead of promising token savings.
The adapter retains the declared HTTP(S) forward-proxy route and `NO_PROXY`
exclusions; it does not bypass an isolated task's model egress boundary.

The initial work context requests a local probe before first submission. The
installed `loopx-research-probe` uses the staged runner/Python:

```sh
loopx-research-probe --candidate-file candidate.py --output probe-001.json \
  --input-revision public-case-v1 --planned-method-id scheduler \
  --executed-method-id mask-selection --change-reason 'The control isolated mask selection first.' \
  --evidence-ref probe:local-001 --limit-seconds 30 --diagnostics probe-diagnostics.json \
  -- python3 candidate.py
```

The solver may write `{"search_iterations":2,"stop_reason":"search_cap"}` to
`LOOPX_RESEARCH_DIAGNOSTICS`. The host measures elapsed time, exit/timeout and
the selected candidate-file digest before/after. A changed candidate rejects
attribution. Solver counters/reasons remain reports, missing counters remain
null, and low resource use is a diagnostic rather than a mandatory time floor.
Pins cover only declared files at those boundaries, not transitive imports or
independent writers. Evidence paths must be fresh; raw output stays local.
The probe neither submits nor accesses hidden evaluation.

The adjacent `.candidate.json` preserves the selected relative paths and file
hashes. `candidate_sha256` hashes that sorted JSON file manifest (sorted keys,
compact separators), not a native archive's container bytes. Compare the same
manifest against the submitted/released snapshot before attaching feedback;
an archive SHA-256 is not interchangeable. If that comparison is unavailable,
leave result attribution unclaimed rather than copying the current hash.

Attach the emitted object as `execution_observation` in the existing Explore
result writeback, preserving its input revision and evidence refs. For released
feedback, add `result_ref` to those refs and `result_candidate_sha256` naming the
evaluated candidate. A different hash is rejected; late feedback belongs on its
actual older candidate, not newer files. Different planned/executed method ids
require a change reason. Replan and the research-history page read the same
structured source; neither certifies method benefit or Goal completion.

This treatment does not automatically instrument arbitrary solvers or guarantee
a stronger first score. The model may omit a probe or misreport a method; actual
adoption and benefit still require qualification. Stage separation does not
replace the default whole-Goal acceptance owner. Do not extend budget or suppress
legal closeout to manufacture a successful long run.

中文：这一显式处理将初始候选、持续研究、等待准入、执行证据与复盘接在既有
Todo/Host/Explore 路径上。候选完成只释放后续研究；已知等待阻止新上游请求。
探针记录候选指纹、耗时和可选搜索诊断；反馈按实际候选绑定，改向保留原因。
未知不填零，不改变评分，不强制耗满时间，不保证模型采纳或最终收益。
