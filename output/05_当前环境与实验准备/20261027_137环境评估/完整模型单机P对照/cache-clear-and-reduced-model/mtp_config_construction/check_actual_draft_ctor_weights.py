"""Actual AST config -> loader -> MTP constructor/weight writes, CPU only."""
from __future__ import annotations
import ast
import copy
from contextlib import nullcontext
from dataclasses import dataclass, replace
import inspect
import json
from pathlib import Path
import sys
import time
from types import ModuleType, SimpleNamespace as NS
from types import MappingProxyType

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
SOURCES = []


def function(source, name, namespace):
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
    node.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])), name, "exec"), namespace)
    return namespace[name]


def cls(source, name, methods, namespace, bases=()):
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == name)
    node.bases = [ast.Name(id=base, ctx=ast.Load()) for base in bases]
    node.decorator_list = []
    node.body = [n for n in node.body if isinstance(n, ast.FunctionDef) and n.name in methods]
    assert {n.name for n in node.body} == set(methods)
    for method in node.body:
        SOURCES.append({"class": name, "method": method.name, "begin": method.lineno, "end": method.end_lineno})
        method.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])), name, "exec"), namespace)
    return namespace[name]


class Module:
    def __init__(self): pass
    def eval(self): return self


class Param:
    def __init__(self, name): self.name, self.value = name, None


class Device:
    def __enter__(self): return self
    def __exit__(self, *args): pass


@dataclass
class ModelConfig:
    hf_config: object
    model: str
    quantization: object
    dtype: str = "CPU fixture"


@dataclass
class Config:
    model_config: object
    speculative_config: object
    attention_config: object
    kernel_config: object
    quant_config: object
    parallel_config: object
    compilation_config: object
    device_config: object
    load_config: object


@dataclass
class Attention:
    backend: object = "target"


@dataclass
class Kernel:
    moe_backend: object = "target"


def main():
    raw = (BASE / "actual_parent_proposer_and_loader.txt").read_text(encoding="utf-8-sig")
    first, second = raw.split("FILE /vllm-workspace/vllm/vllm/model_executor/model_loader/base_loader.py", 1)
    decode = lambda text: "\n".join(line.partition(":")[2] for line in text.splitlines() if line.partition(":")[0].isdigit())
    parent_source, loader_source = decode(first), decode(second)
    mtp_source = (BASE / "model_sources_deepseek_mtp.py").read_text(encoding="utf-8")
    patch_source = (BASE / "pd_actual_patch_deepseek_mtp.py").read_text(encoding="utf-8")
    metadata = json.loads((BASE / "shard_closure_metadata.json").read_text(encoding="utf-8"))
    input_names = metadata["mtp_special_weight_names"] + ["rot.weight"]
    assert len(input_names) == 7 and len(set(input_names)) == 7
    parameters = [name.removeprefix("model.layers.78.") for name in input_names if name != "rot.weight" and "embed_tokens" not in name] + ["rot.weight"]
    writes, selected_configs, passed_configs = [], [], []
    def write(param, value, *args):
        param.value = value
        writes.append({"input": value, "destination": param.name})
    class Layer:
        def __init__(self, config, prefix):
            self.params = {prefix + "." + name: Param(prefix + "." + name) for name in parameters}
    class Embedding:
        def __init__(self, *args, prefix): self.weight = Param(prefix + ".weight")
    logger = NS(info=lambda *a: None, info_once=lambda *a: None, debug=lambda *a: None)
    env = dict(Module=Module, copy=copy, replace=replace, inspect=inspect, time=time, logger=logger,
        torch=NS(nn=NS(ModuleDict=dict), device=lambda value: Device()),
        DeepSeekMultiTokenPredictorLayer=Layer, VocabParallelEmbedding=Embedding,
        LogitsProcessor=lambda *a: None, maybe_prefix=lambda prefix, suffix: prefix + "." + suffix if prefix else suffix,
        MTP_ROT_WEIGHT_NAME="rot.weight", rocm_aiter_ops=NS(is_fusion_moe_shared_experts_enabled=lambda: False),
        fused_moe_make_expert_params_mapping=lambda *a, **k: [], get_pp_missing_layer_names=lambda model: set(),
        _try_load_fp8_indexer_wk=lambda *a: False, maybe_remap_kv_scale_name=lambda name, params: name,
        default_weight_loader=write, set_current_vllm_config=lambda *a, **k: nullcontext(),
        record_metadata_for_reloading=lambda model: None, configure_quant_config=lambda *a: None,
        set_default_torch_dtype=lambda dtype: nullcontext(), log_model_inspection=lambda model: None,
        current_platform=NS(is_cuda_alike=lambda: False), _has_online_quant=lambda model: False,
        process_weights_after_loading=lambda *a: None)
    env["get_spec_layer_idx_from_weight_name"] = function(patch_source, "get_spec_layer_idx_from_weight_name", env)
    predictor = cls(mtp_source, "DeepSeekMultiTokenPredictor", {"__init__"}, env, ("Module",))
    model_type = cls(mtp_source, "DeepSeekMTP", {"__init__", "load_weights", "_rewrite_spec_layer_name"}, env, ("Module",))
    model_type.set_moe_parameters = lambda self: None
    def named_parameters(model):
        values = [(name, param) for layer in model.model.layers.values() for name, param in layer.params.items()]
        values.append((model.model.embed_tokens.weight.name, model.model.embed_tokens.weight))
        return values
    model_type.named_parameters = named_parameters
    model_type.named_modules = lambda self: []
    ascend_type = cls(patch_source, "AscendDeepSeekMTP", {"_rewrite_spec_layer_name"}, env, ("DeepSeekMTP",))
    def architecture(config):
        selected_configs.append(config)
        return ascend_type, "DeepSeekMTP"
    env["get_model_architecture"] = architecture
    env["initialize_model"] = function((BASE / "extra_model_loader_utils.py").read_text(encoding="utf-8"), "initialize_model", env)
    base_loader = cls(loader_source, "BaseModelLoader", {"__init__", "load_model"}, env)
    default_loader = cls((BASE / "model_sources_default_loader.py").read_text(encoding="utf-8"),
        "DefaultModelLoader", {"load_weights", "track_weights_loading"}, env, ("BaseModelLoader",))
    default_loader._init_ep_weight_filter = lambda *a: None
    default_loader.get_all_weights = lambda *a: [(name, name) for name in input_names]
    parent = cls(parent_source, "SpecDecodeBaseProposer", {"_create_draft_vllm_config"}, env)
    backends = ModuleType("vllm.compilation.backends")
    backends.set_model_tag = lambda tag: nullcontext()
    sys.modules[backends.__name__] = backends
    original_proposer = cls((BASE / "ascend_llm_base_proposer.py").read_text(encoding="utf-8"), "AscendSpecDecodeBaseProposer", {"_get_model"}, env)
    repaired_path = BASE / "reduced-pd-prepared/runtime/draft-n16/llm_base_proposer_n16.py"
    repaired_proposer = cls(repaired_path.read_text(encoding="utf-8"), "AscendSpecDecodeBaseProposer", {"_get_model"}, env)
    results = []
    for label, target_n, quantization, repaired, force_track in (
        ("broken_target16_draft78_none", 16, None, False, True),
        ("quantization_only_does_not_fix_writes", 16, "ascend", False, None),
        ("private_copy_fix_strict_all_seven_written", 16, "ascend", True, True),
        ("original78_private_guard_unchanged", 78, "ascend", True, True),
    ):
        writes.clear(); selected_configs.clear(); passed_configs.clear()
        hf = lambda n: NS(num_hidden_layers=n, num_nextn_predict_layers=1, model_type="glm_moe_dsa",
                          vocab_size=64, hidden_size=16, n_routed_experts=0, n_shared_experts=0)
        target = ModelConfig(hf(target_n), "target-view" if target_n == 16 else "original-78", "ascend")
        draft = ModelConfig(hf(78), "original-78", quantization)
        draft.hf_config.model_type = "deepseek_mtp"
        draft.hf_text_config = draft.hf_config
        spec = NS(method="mtp", draft_model_config=draft, draft_load_config=NS(load_format=None),
                  target_model_config=target, moe_backend=None, attention_backend=None)
        config = Config(target, spec, Attention(), Kernel(), object(), object(), object(), NS(device="cpu"), NS(device=None))
        loader = default_loader(config.load_config)
        loader.counter_before_loading_weights, loader.enable_weights_track = time.perf_counter(), force_track
        def get_model(*, vllm_config, model_config, load_config):
            passed_configs.append(vllm_config)
            model = env["initialize_model"](vllm_config, model_config=model_config)
            passed_configs.append(model)
            # Use the real BaseModelLoader too, so both constructor and loading
            # run through its actual call path exactly once below.
            return loader.load_model(vllm_config, model_config)
        # Avoid a duplicate construction in get_model; model capture comes from
        # the same actual initialize_model called by the real base loader.
        real_initialize = env["initialize_model"]
        def capture_initialize(*args, **kwargs):
            model = real_initialize(*args, **kwargs)
            passed_configs.append(model)
            return model
        env["initialize_model"] = capture_initialize
        def get_model(*, vllm_config, model_config, load_config):
            passed_configs.append(vllm_config)
            return loader.load_model(vllm_config, model_config)
        env["get_model"] = get_model
        proposer = NS(vllm_config=config, speculative_config=spec, method="mtp")
        proposer._create_draft_vllm_config = lambda: parent._create_draft_vllm_config(proposer)
        failure = None
        try: (repaired_proposer if repaired else original_proposer)._get_model(proposer)
        except ValueError as error: failure = str(error)
        env["initialize_model"] = real_initialize
        built, call_config = passed_configs[-1], passed_configs[0]
        assert config.model_config is target and target.hf_config.num_hidden_layers == target_n
        assert all(getattr(call_config, attr) is getattr(config, attr) for attr in ("quant_config", "parallel_config", "compilation_config"))
        assert all(value is draft for value in selected_configs)
        row = {"case": label, "constructed_layers": list(built.model.layers), "writes": list(writes),
               "not_written": [name for name, param in built.named_parameters() if param.value is None],
               "strict_failure": failure, "target_unchanged": True, "runtime_config_identities_preserved": True}
        if target_n == 16 and not repaired:
            assert list(built.model.layers) == ["16"] and len(writes) == 1
            assert writes[0] == {"input": "rot.weight", "destination": "model.layers.16.rot.weight"}
            assert len(row["not_written"]) == 6
            assert (failure is not None) == (quantization is None)
        else:
            assert list(built.model.layers) == ["78"] and len(writes) == 7
            assert not row["not_written"] and failure is None
            if target_n == 78: assert call_config.model_config is target
            else:
                assert call_config.model_config is not draft
                assert call_config.model_config.hf_config is not draft.hf_config
                assert call_config.model_config.hf_config.model_type == "glm_moe_dsa"
                assert call_config.model_config.hf_text_config.model_type == "glm_moe_dsa"
                assert draft.hf_config.model_type == "deepseek_mtp"
                assert draft.hf_text_config is draft.hf_config
                row["construction_model_type"] = call_config.model_config.hf_config.model_type
                row["construction_num_layers"] = call_config.model_config.hf_config.num_hidden_layers
                row["spec_draft_unchanged"] = True
        results.append(row)
    quant_source = (BASE / "quant_actual_modelslim_config.py").read_text(encoding="utf-8")
    quant_tree = ast.parse(quant_source)
    packed = next(node for node in quant_tree.body if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "packed_modules_model_mapping")
    mapping = ast.literal_eval(packed.value)
    real_quant = json.loads((BASE / "draft78_quant_real_metadata.json").read_text(encoding="utf-8"))
    desc = real_quant["keys"]
    prefix = "model.layers.78.self_attn.fused_qkv_a_proj"
    assert prefix + ".weight" not in desc
    class LinearBase: pass
    class Scheme: pass
    class Method:
        def __init__(self, scheme): self.scheme = scheme
    adapters = ModuleType("ks_cpu_quant.method_adapters")
    for name in ("AscendLinearMethod", "AscendEmbeddingMethod", "AscendFusedMoEMethod", "AscendKVCacheMethod"):
        setattr(adapters, name, Method)
    sys.modules[adapters.__name__] = adapters
    qenv = {"__name__": "ks_cpu_quant.config", "__package__": "ks_cpu_quant",
        "MappingProxyType": MappingProxyType, "packed_modules_model_mapping": mapping,
        "LinearBase": LinearBase, "logger": logger,
        "get_scheme_class": lambda quant_type, layer_type: Scheme if (quant_type, layer_type) == ("W8A8", "linear") else None}
    for constant in ("QUANT_MODEL_PREFIX_MAPPINGS", "QUANT_MODEL_SUBSTR_MAPPINGS"):
        node = next(n for n in quant_tree.body if isinstance(n, ast.Assign) and any(getattr(target, "id", None) == constant for target in n.targets))
        qenv[constant] = ast.literal_eval(node.value)
    for name in ("get_linear_quant_type", "get_quant_type_for_layer", "create_scheme_for_layer"):
        function(quant_source, name, qenv)
    quant_cls = cls(quant_source, "AscendModelSlimConfig", {"get_quant_method", "is_layer_skipped_ascend", "quant_prefix_mapper", "_has_quant_weight"}, qenv)
    dispatch = []
    for family in ("deepseek_mtp", "glm_moe_dsa"):
        qenv["get_current_vllm_config"] = lambda family=family: NS(model_config=NS(hf_config=NS(model_type=family)))
        quant = quant_cls()
        quant.quant_description, quant.packed_modules_mapping = dict(desc), {}
        try:
            method = quant.get_quant_method(LinearBase(), prefix)
            assert family == "glm_moe_dsa" and isinstance(method, Method)
            assert quant.packed_modules_mapping["fused_qkv_a_proj"] == ["q_a_proj", "kv_a_proj_with_mqa"]
            assert qenv["get_linear_quant_type"](desc, prefix, quant.packed_modules_mapping) == "W8A8"
            dispatch.append({"family": family, "status": "W8A8_DISPATCH_SUCCESS_REAL_TWO_KEYS", "fused_mapping": quant.packed_modules_mapping["fused_qkv_a_proj"]})
        except KeyError as error:
            assert family == "deepseek_mtp" and error.args == (prefix + ".weight",)
            dispatch.append({"family": family, "status": "EXPECTED_MISSING_FUSED_KEY", "key": error.args[0]})
    report = {"status": "PASS", "evidence_class": "ACTUAL_AST_FAMILY_QUANT_CTOR_WEIGHT_WRITES_CPU_ONLY", "cases": results,
        "actual_methods": SOURCES, "source_weight_names": input_names,
        "native_parent_create_draft_model_config_replaced": False,
        "private_fix": "guarded clone draft ModelConfig plus deepcopied HF; retain original78 derived fields and restore target glm_moe_dsa family only in construction context",
        "construction_model_type": "glm_moe_dsa", "construction_num_layers": 78,
        "special_weights_written": 7, "quant_packed_fused_qkv_present": True,
        "quant_dispatch": dispatch, "quant_description_source": real_quant,
        "fixtures": ["dataclass configuration containers", "nn modules/parameters and scalar weight markers", "device/context and quant postprocess", "MoE/expert and FP8 helpers not exercised"],
        "real_checkpoint_bytes_loaded": False, "npu_actions": 0, "installed_source_changes": 0}
    (BASE / "draft_n16_family_quant_cpu.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "cases": len(results), "fixed_writes": results[2]["writes"]}, ensure_ascii=False))


if __name__ == "__main__": main()
