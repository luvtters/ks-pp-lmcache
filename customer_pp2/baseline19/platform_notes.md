# Platform component

`platform_plan(vllm_root, ascend_root)` returns changed absolute `Path` keys and
UTF-8 `bytes` values. It reads source and compiles proposed Python text without
importing vLLM, Ascend, or accelerator libraries. It never writes, installs,
starts, or stops anything. Both roots may be the same `site-packages` directory.

The plan contains these measured changes:

- Native engine/executor/shared-memory admission while a model step is running.
- Base scheduler and the customer's existing Ascend `KVDeliveryScheduler`
  integration. `KS_LMCACHE_LOOKUP_TAIL_20MS=0` keeps the optional tail wait off.
- EPLB warmup receive buffers and completion synchronization.
- Headless executor import inside `run_headless`, after the Ascend platform has
  replaced `MultiprocExecutor`. Existing native `vllm serve` usage is preserved.
- Worker-internal mirror readiness and `wait_for_pool`, replacing the old
  external gate. The core component must provide
  `lmcache.v1.multiprocess.bifrost_order.wait_for_pool`.
- The optional, measured Ascend admission observer. It only writes when
  `KS_ADMIT_DIAG_DIR` is explicitly configured.

The caller applies `PLATFORM_ENV`, unsets `KS_ORDER_GATE_DIR`, and supplies the
node-specific `LMCACHE_BIFROST_ORDER`. This component does not set runtime
environment variables. It does not change model loading, offload, or STORE
policy.

The customer's Bifrost manager, Ascend scheduler backport, and non-daemon
`AscendMultiprocExecutor` are prerequisites. The plan checks the mirror readiness
interface and executor patch before returning changes. If these are missing or
an integration hunk differs, it raises `PlatformCompatibilityError` and returns
no plan; the installer should stop and report the mismatch. It does not replace
those customer implementations or guess an alternative executor.

Static verification uses private source fixtures supplied outside this package:

```text
python platform_test.py <fixture-root>
```

The fixture has `vllm/` and `vllm-ascend/` source roots. Checks cover the captured
candidate, the original state reconstructed by reversing the exact delivered
patches, previous FULL plus gate, repeat installation, preservation of unrelated
edits, rejection of a modified hook without writes, and shared package roots.
Planned Python is compiled. These checks do not start an accelerator runtime or
establish a fresh device/performance result. Removing the inactive old gate
fallback has only static verification in this delivery.

The bundled files are incremental patches and the planner. Customer source,
private diagnostics, raw logs, addresses, and account details are not included.
