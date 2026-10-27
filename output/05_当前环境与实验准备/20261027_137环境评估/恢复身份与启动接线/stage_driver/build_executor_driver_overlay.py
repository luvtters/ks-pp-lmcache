"""Rebase only stage execution seams onto the actual 137 source snapshots."""
from __future__ import annotations
import ast
import difflib
import json
from pathlib import Path
import shutil

HERE = Path(__file__).resolve().parent
IDENTITY = HERE.parent
ASSESSMENT = IDENTITY.parent
BASE = ASSESSMENT / "完整模型单机P对照"
OUT = HERE / "runtime_overlay"
PROTO = next(p for p in HERE.parents if p.name == "output") / "04_修改策略与验证/01_修改策略与隔离候选/20261027_PP分段就绪原型"


def method(text, cls, name):
    tree = ast.parse(text)
    owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls)
    node = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == name)
    begin = min([node.lineno] + [d.lineno for d in node.decorator_list]) - 1
    return "".join(text.splitlines(True)[begin:node.end_lineno]), begin, node.end_lineno


def replace_method(text, cls, name, replacement):
    _, begin, end = method(text, cls, name)
    lines = text.splitlines(True)
    lines[begin:end] = [replacement.rstrip() + "\n"]
    return "".join(lines)


def append_methods(text, cls, snippets):
    owner = next(n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == cls)
    lines = text.splitlines(True)
    lines.insert(owner.end_lineno, "\n\n" + "\n\n".join(s.rstrip() for s in snippets) + "\n")
    return "".join(lines)


def once(text, old, new):
    assert text.count(old) == 1, (old[:110], text.count(old))
    return text.replace(old, new, 1)


def write(relative, text, original=None):
    ast.parse(text)
    target = OUT / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8", newline="\n")
    if original is not None:
        patch = "".join(difflib.unified_diff(original.splitlines(True), text.splitlines(True),
                    fromfile="actual137/" + relative, tofile="private/" + relative))
        (HERE / (target.stem + ".driver.patch")).write_text(patch, encoding="utf-8", newline="\n")


def build():
    donor_ex = (IDENTITY / "overlay/vllm/v1/executor/multiproc_executor.py").read_text(encoding="utf-8")
    donor_core = (IDENTITY / "overlay/vllm/v1/engine/core.py").read_text(encoding="utf-8")
    original_ex = (BASE / "pp_executor.py").read_text(encoding="utf-8")
    original_core = (BASE / "pp_core.py").read_text(encoding="utf-8")
    ex = original_ex
    ex = once(ex, "logger = init_logger(__name__)\n", "from ks_stage_executor_driver import (KSStageRPCCommand, KSStageWireReply, FirstTokenStageDriver, handle_worker_command)\n\nlogger = init_logger(__name__)\n")
    prefix = donor_ex[donor_ex.index("_KS_STAGE_METHODS ="):donor_ex.index("class FutureWrapper(")]
    ex = once(ex, "class FutureWrapper(", prefix + "class FutureWrapper(")
    snippets = [method(donor_ex, "MultiprocExecutor", n)[0] for n in (
        "_init_stage_event_channels", "submit_stage_command", "poll_stage_events", "_close_stage_event_channels")]
    ex = append_methods(ex, "MultiprocExecutor", snippets)
    init = method(ex, "MultiprocExecutor", "_init_executor")[0]
    init = once(init, "        context = get_mp_context()\n", "        context = get_mp_context()\n        self._init_stage_event_channels(context, tp_size, pp_size, pcp_size)\n")
    init = once(init, "                        inherited_fds=inherited_fds,\n", "                        inherited_fds=inherited_fds,\n                        stage_event_channel=self._ks_stage_channels.get(global_rank),\n")
    ex = replace_method(ex, "MultiprocExecutor", "_init_executor", init)
    ex = append_methods(ex, "WorkerProc", [method(donor_ex, "WorkerProc", n)[0] for n in ("_init_stage_event_channel", "_handle_stage_command")])
    worker_init = method(ex, "WorkerProc", "__init__")[0]
    worker_init = once(worker_init, "        is_driver_worker: bool,\n", "        is_driver_worker: bool,\n        stage_event_channel=None,\n")
    worker_init = once(worker_init, "        self._init_message_queues(input_shm_handle, vllm_config)\n", "        self._init_message_queues(input_shm_handle, vllm_config)\n        self._init_stage_event_channel(stage_event_channel, vllm_config)\n")
    ex = replace_method(ex, "WorkerProc", "__init__", worker_init)
    make = method(ex, "WorkerProc", "make_worker_process")[0]
    make = once(make, "        inherited_fds: list[int] | None = None,\n", "        inherited_fds: list[int] | None = None,\n        stage_event_channel=None,\n")
    make = once(make, '            "is_driver_worker": is_driver_worker,\n', '            "is_driver_worker": is_driver_worker,\n            "stage_event_channel": stage_event_channel,\n')
    ex = replace_method(ex, "WorkerProc", "make_worker_process", make)
    busy = method(ex, "WorkerProc", "worker_busy_loop")[0]
    busy = once(busy, "            try:\n", "            if isinstance(method, KSStageRPCCommand):\n                handle_worker_command(self, method, (ModelRunnerOutput, AsyncModelRunnerOutput), AsyncModelRunnerOutput)\n                continue\n            if isinstance(method, KSStageCommand):\n                self._handle_stage_command(method)\n                continue\n            try:\n")
    ex = replace_method(ex, "WorkerProc", "worker_busy_loop", busy)
    poll = method(ex, "MultiprocExecutor", "poll_stage_events")[0]
    poll = once(poll, "                if not isinstance(event, (KSStageEvent, KSRestoreEvent)):\n", "                if isinstance(event, KSStageWireReply):\n                    event.validate((ModelRunnerOutput, AsyncModelRunnerOutput))\n                    if (event.worker_id != rank or event.stage != rank // self._ks_stage_tp_size\n                            or event.sequence != self._ks_stage_sequences.get(rank, 0) + 1):\n                        raise ValueError('typed stage worker/sequence mismatch')\n                    self._ks_stage_driver.accept(event)\n                    self._ks_stage_sequences[rank] = event.sequence\n                    continue\n                if not isinstance(event, (KSStageEvent, KSRestoreEvent)):\n")
    poll = once(poll, "        return events\n", "        if driver := getattr(self, '_ks_stage_driver', None):\n            driver.pump()\n        return events\n")
    ex = replace_method(ex, "MultiprocExecutor", "poll_stage_events", poll)
    execute = method(ex, "MultiprocExecutor", "execute_model")[0]
    sample = method(ex, "MultiprocExecutor", "sample_tokens")[0]
    ex = append_methods(ex, "MultiprocExecutor", [execute.replace("def execute_model(", "def _ks_legacy_execute_model(", 1), sample.replace("def sample_tokens(", "def _ks_legacy_sample_tokens(", 1)])
    ex = replace_method(ex, "MultiprocExecutor", "execute_model", '''    def execute_model(self, scheduler_output, non_block=False):
        driver = getattr(self, "_ks_stage_driver", None)
        if driver is None:
            return self._ks_legacy_execute_model(scheduler_output, non_block=non_block)
        if not hasattr(self, "_ks_sample_routes"):
            self._ks_sample_routes = deque()
        tag = getattr(scheduler_output, "ks_stage_batch", None)
        if scheduler_output.total_num_scheduled_tokens:
            self._ks_sample_routes.append("stage" if tag else "legacy")
        if tag:
            if not non_block:
                raise ValueError("staged execute requires the core's existing non-blocking call")
            return driver.admit(scheduler_output)
        if driver.batches:
            return driver.defer_legacy(lambda: self._ks_legacy_execute_model(scheduler_output, non_block=True))
        return self._ks_legacy_execute_model(scheduler_output, non_block=non_block)
''')
    ex = replace_method(ex, "MultiprocExecutor", "sample_tokens", '''    def sample_tokens(self, grammar_output, non_block=False):
        driver = getattr(self, "_ks_stage_driver", None)
        if driver is None:
            return self._ks_legacy_sample_tokens(grammar_output, non_block=non_block)
        route = self._ks_sample_routes.popleft()
        if route == "stage":
            if not non_block:
                raise ValueError("staged sample requires the core's existing non-blocking call")
            return driver.request_sample(grammar_output)
        if driver.batches:
            return driver.defer_legacy(lambda: self._ks_legacy_sample_tokens(grammar_output, non_block=True))
        return self._ks_legacy_sample_tokens(grammar_output, non_block=non_block)
''')
    ex = append_methods(ex, "MultiprocExecutor", ['''    def ks_bind_stage_driver(self, controller, *, async_scheduling):
        if os.environ.get("KS_PP_STAGE_DRIVER") != "1":
            return
        self._ks_stage_driver = FirstTokenStageDriver(self, controller,
            (ModelRunnerOutput, AsyncModelRunnerOutput), async_scheduling=async_scheduling)

    def ks_ack_stage_result(self, scheduler_output, model_output):
        driver = getattr(self, "_ks_stage_driver", None)
        if driver is not None:
            driver.core_consumed(scheduler_output, model_output)
'''])
    write("vllm/v1/executor/multiproc_executor.py", ex, original_ex)

    core = original_core
    core = once(core, "import time\n", "import time\nimport weakref\n")
    reader_start = donor_core.index("class _KSModelResultReader")
    reader_end = donor_core.index("class EngineCore:")
    core = once(core, "class EngineCore:\n", donor_core[reader_start:reader_end] + "class EngineCore:\n")
    helpers = [method(donor_core, "EngineCore", n)[0] for n in (
        "configure_ks_core_event_pump", "ks_model_wait_inflight", "_ks_assert_no_event_callback_step",
        "_ks_consume_stage_events", "_ks_wait_model_future", "ks_draft_wait_pending",
        "_ks_wait_executor_future", "_ks_close_core_event_pump")]
    core = append_methods(core, "EngineCore", helpers)
    startup = method(donor_core, "EngineCore", "_ks_initialize_restore_notifications")[0]
    startup = startup.replace("# This hook registers facts only. No stage driver or model admission.", "# Bind the real restore facts and the explicitly enabled first-token driver.")
    startup = startup.replace("from vllm.v1.engine.ks_stage_controller import StageController", "from span_stage_controller import StageController")
    startup = once(startup, "        self.configure_ks_core_event_pump(binding)\n", "        from ks_stage_executor_driver import restore_trace_callback\n        self.configure_ks_core_event_pump(restore_trace_callback(binding, controller, KSRestoreEvent))\n")
    startup = once(startup, "        self._ks_restore_notifications_only = True\n", "        self._ks_restore_notifications_only = True\n        if os.environ.get('KS_PP_STAGE_DRIVER') == '1':\n            if self.async_scheduling is not True:\n                raise ValueError('first-token stage wire requires actual async scheduling')\n            self.scheduler.ks_bind_stage_controller(controller)\n            self.model_executor.ks_bind_stage_driver(controller, async_scheduling=self.async_scheduling)\n            self._ks_restore_notifications_only = False\n")
    core = append_methods(core, "EngineCore", [startup])
    init = method(core, "EngineCore", "__init__")[0].rstrip() + "\n        self._ks_initialize_restore_notifications()\n"
    core = replace_method(core, "EngineCore", "__init__", init)
    for name in ("step", "step_with_batch_queue"):
        body = method(core, "EngineCore", name)[0]
        body = once(body, "            scheduler_output = self.scheduler.schedule()\n" if name == "step_with_batch_queue" else "        scheduler_output = self.scheduler.schedule()\n",
                    ("            " if name == "step_with_batch_queue" else "        ") + "if getattr(self, '_ks_result_reader', None) is not None:\n" + ("                " if name == "step_with_batch_queue" else "            ") + "self._ks_consume_stage_events()\n" + ("            " if name == "step_with_batch_queue" else "        ") + "scheduler_output = self.scheduler.schedule()\n")
        body = body.replace("model_output = future.result()", "model_output = self._ks_wait_model_future(future)")
        body = body.replace("exec_model_fut.result()", "self._ks_wait_model_future(exec_model_fut)")
        body = once(body, "            scheduler_output, model_output\n        )\n", "            scheduler_output, model_output\n        )\n        self.model_executor.ks_ack_stage_result(scheduler_output, model_output)\n")
        core = replace_method(core, "EngineCore", name, body)
    write("vllm/v1/engine/core.py", core, original_core)
    write("vllm/v1/engine/ks_restore_binding.py", (IDENTITY / "overlay/vllm/v1/engine/ks_restore_binding.py").read_text(encoding="utf-8"))
    for name in ("typed_stage_protocol.py", "span_stage_controller.py"):
        write(name, (HERE / name).read_text(encoding="utf-8"))

    # Current 137 worker is the base; only add the reviewed stage APIs and
    # the receive-buffer branch. Never substitute a historical whole worker.
    raw = (ASSESSMENT / "真实KV恢复验证/actual137_worker_source.txt").read_text(encoding="utf-8")
    worker = "\n".join(line.partition(":")[2] for line in raw.splitlines() if line.partition(":")[0].isdigit()) + "\n"
    original_worker = worker
    donor_worker = (PROTO / "worker_split/worker.py").read_text(encoding="utf-8")
    worker = append_methods(worker, "NPUWorker", [method(donor_worker, "NPUWorker", n)[0] for n in (
        "_ks_pp_batch_signature", "_ks_pp_stage_record", "_ks_pp_handles_done", "prepare_pp_stage_input",
        "pp_stage_input_received", "finalize_pp_stage_input", "pp_stage_input_ready", "execute_pp_stage_model",
        "pp_stage_model_ready", "pp_stage_release_ready", "release_pp_stage_batch")])
    # A false query is pending; an actual device/communication exception must
    # become a retained typed failure, not an endless readiness poll.
    handles = method(worker, "NPUWorker", "_ks_pp_handles_done")[0]
    handles = once(handles, "            try:\n                if probe() is not True:\n                    return False\n            except Exception:\n                return False\n",
                   "            if probe() is not True:\n                return False\n")
    worker = replace_method(worker, "NPUWorker", "_ks_pp_handles_done", handles)
    for query_name in ("pp_stage_input_ready", "pp_stage_model_ready"):
        query = method(worker, "NPUWorker", query_name)[0]
        query = once(query, "        try:\n            return event.query() is True\n        except Exception:\n            return False\n", "        return event.query() is True\n")
        worker = replace_method(worker, "NPUWorker", query_name, query)
    release_query = method(worker, "NPUWorker", "pp_stage_release_ready")[0]
    release_query = once(release_query, '            try:\n                send_ready = record["send_event"].query() is True\n            except Exception:\n                send_ready = False\n', '            send_ready = record["send_event"].query() is True\n')
    worker = replace_method(worker, "NPUWorker", "pp_stage_release_ready", release_query)
    worker = once(worker, "import copy\n", "import copy\nfrom ks_stage_executor_driver import trace_worker\n")
    record_method = method(worker, "NPUWorker", "_ks_pp_stage_record")[0]
    record_method = once(record_method, '                "key": batch_key,\n', '                "key": batch_key,\n                "stage_tag": getattr(scheduler_output, "ks_stage_batch", None),\n                "stage": 0 if get_pp_group().is_first_rank else 1,\n')
    worker = replace_method(worker, "NPUWorker", "_ks_pp_stage_record", record_method)
    stage_execute = method(worker, "NPUWorker", "execute_pp_stage_model")[0]
    stage_execute = once(stage_execute, "            result = self.execute_model(scheduler_output)\n", "            trace_worker(self, batch_key, 'worker_stage_execute_begin')\n            result = self.execute_model(scheduler_output)\n")
    worker = replace_method(worker, "NPUWorker", "execute_pp_stage_model", stage_execute)
    model_ready = method(worker, "NPUWorker", "pp_stage_model_ready")[0]
    model_ready = once(model_ready, "        return event.query() is True\n", "        ready = event.query() is True\n        if ready and not record.get('trace_model_complete'):\n            trace_worker(self, batch_key, 'worker_model_device_complete_observed')\n            record['trace_model_complete'] = True\n        return ready\n")
    worker = replace_method(worker, "NPUWorker", "pp_stage_model_ready", model_ready)
    # Keep the current execute body and modify only exact receive/send anchors.
    body = method(worker, "NPUWorker", "execute_model")[0]
    body = once(body, "        self.log_memory_stats()\n", "        if (getattr(self, '_ks_pp_stage_current', None) is not None\n                and getattr(self, '_ks_pp_stage_running', None) is None):\n            raise RuntimeError('stage batch owns worker state; legacy execute cannot overwrite it')\n        self.log_memory_stats()\n")
    begin = body.index("            tensor_dict, comm_handles, comm_postprocess = get_pp_group().irecv_tensor_dict(")
    finish = body.index("\n\n        if self.profiler", begin)
    receive = body[begin:finish]
    body = body[:begin] + "            stage_record = getattr(self, '_ks_pp_stage_running', None)\n            if stage_record is not None:\n                intermediate_tensors = stage_record['input']\n            else:\n" + "\n".join("    " + line if line else line for line in receive.split("\n")) + body[finish:]
    body = once(body, "        self._pp_send_work = get_pp_group().isend_tensor_dict(\n", "        if getattr(self, '_ks_pp_stage_running', None) is not None:\n            self._ks_pp_retained_send_tensors = output.tensors\n        self._pp_send_work = get_pp_group().isend_tensor_dict(\n")
    worker = replace_method(worker, "NPUWorker", "execute_model", body)
    write("vllm_ascend/worker/worker.py", worker, original_worker)
    manifest = {"status": "BUILT_ACTUAL137_PRIVATE_OVERLAY_NOT_DEPLOYED", "vllm_commit": "0fc695fc6d1d82e9a5ac6835ac8e4e1c83703665",
                "ascend_commit": "5cb98caaadeff42b5b62b996e34bb2aaa29d20fd", "scope": "max_tokens=1 no scheduled spec actual async=True",
                "base": [str(BASE / "pp_core.py"), str(BASE / "pp_executor.py"), "actual137_worker_source.txt"],
                "common_loader": "root combines; no sitecustomize overwritten", "model_runner_override": "common local-completion canonical module remains authoritative",
                "files": [str(p.relative_to(OUT)) for p in OUT.rglob("*.py")], "npu_actions": 0}
    (HERE / "executor_driver_build.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__": build()
