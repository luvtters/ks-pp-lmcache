"""Prepare the private N16/original78 draft-construction fix; never starts models."""
from pathlib import Path
import argparse
import ast
import base64
import difflib
import json
import shlex
import subprocess
import sys

HERE=Path(__file__).resolve().parent
OUT=HERE/'reduced-pd-prepared'
REMOTE='/opt/ks-stage-ready-luv-20261027/reduced-pd'
RELATIVE='runtime/draft-n16/llm_base_proposer_n16.py'
CANONICAL='vllm_ascend.spec_decode.llm_base_proposer'


def prepare():
    before=(HERE/'ascend_llm_base_proposer.py').read_text(encoding='utf-8')
    anchor='''        draft_vllm_config = self._create_draft_vllm_config()
        draft_load_config = self.speculative_config.draft_load_config'''
    replacement='''        draft_vllm_config = self._create_draft_vllm_config()
        draft_model_config = self.speculative_config.draft_model_config
        if (
            self.speculative_config.method == "mtp"
            and getattr(self.vllm_config.model_config.hf_config, "model_type", None) == "glm_moe_dsa"
            and getattr(self.vllm_config.model_config.hf_config, "num_hidden_layers", None) == 16
            and getattr(draft_model_config.hf_config, "num_hidden_layers", None) == 78
            and getattr(draft_model_config.hf_config, "num_nextn_predict_layers", None) == 1
        ):
            # New-style MTP constructors read vllm_config.model_config directly.
            # Keep target configuration and shared runtime registries unchanged.
            draft_vllm_config = copy.copy(draft_vllm_config)
            construction_model_config = copy.copy(draft_model_config)
            construction_model_config.hf_config = copy.deepcopy(draft_model_config.hf_config)
            draft_text = getattr(draft_model_config, "hf_text_config", draft_model_config.hf_config)
            construction_model_config.hf_text_config = (
                construction_model_config.hf_config if draft_text is draft_model_config.hf_config else copy.deepcopy(draft_text)
            )
            # Match the verified GLM MTP construction family: its official packed
            # map includes fused_qkv_a_proj; deepseek_mtp's generic map does not.
            family = self.vllm_config.model_config.hf_config.model_type
            construction_model_config.hf_config.model_type = family
            construction_model_config.hf_text_config.model_type = family
            draft_vllm_config.model_config = construction_model_config
        draft_load_config = self.speculative_config.draft_load_config'''
    if before.count(anchor)!=1:raise ValueError('actual proposer anchor changed')
    after=before.replace(anchor,replacement)
    ast.parse(after)
    path=OUT/RELATIVE;path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(after,encoding='utf-8',newline='\n')
    patch=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),
                    fromfile='actual137/llm_base_proposer.py',tofile='private/llm_base_proposer_n16.py'))
    (HERE/'draft_config_n16_review.patch').write_text(patch,encoding='utf-8')
    # Native revision uses the official explicit quantization field for an explicit draft path.
    for name in ['p-original-async','p-stage-candidate','d0','d1']:
        path=OUT/f'startup/{name}.serve.native.sh'
        text=path.read_text(encoding='utf-8')
        old='"method":"deepseek_mtp","enforce_eager":true,"model":'
        new='"method":"deepseek_mtp","enforce_eager":true,"quantization":"ascend","model":'
        if old in text:text=text.replace(old,new)
        elif '"quantization":"ascend","model":' not in text:raise ValueError('native spec anchor changed')
        path.write_text(text,encoding='utf-8',newline='\n')
    common=OUT/'startup/common.env.native.sh';text=common.read_text(encoding='utf-8')
    if 'KS_N16_DRAFT_PROPOSER_OVERRIDE' not in text:
        text+=f'export KS_N16_DRAFT_PROPOSER_OVERRIDE={REMOTE}/{RELATIVE}\n'
        common.write_text(text,encoding='utf-8',newline='\n')
    for name in ['runtime/common/sitecustomize.py','runtime/p-original/sitecustomize.py']:
        path=OUT/name;text=path.read_text(encoding='utf-8')
        if 'KS_N16_DRAFT_PROPOSER_OVERRIDE' in text:continue
        text+='''
# Exact private draft-construction override, alongside the common Mooncake mapping.
_ks_n16_draft_source = _ks_n16_os.environ.get("KS_N16_DRAFT_PROPOSER_OVERRIDE")
if _ks_n16_draft_source:
    class _KSN16DraftProposerFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "vllm_ascend.spec_decode.llm_base_proposer":
                return importlib.util.spec_from_file_location(fullname, _ks_n16_draft_source)
            return None
    sys.meta_path.insert(0, _KSN16DraftProposerFinder())
'''
        path.write_text(text,encoding='utf-8',newline='\n')
    print(json.dumps({'private_file':str(OUT/RELATIVE),'remote_file':REMOTE+'/'+RELATIVE,
                      'canonical':CANONICAL,'diff_lines':len(patch.splitlines()),
                      'source_imported':False,'service_action':False},indent=2))


def deploy(source_only=False):
    names=[RELATIVE,'runtime/common/sitecustomize.py','runtime/p-original/sitecustomize.py',
           'startup/common.env.native.sh','README.md']
    names += [f'startup/{name}.serve.native.sh' for name in ['p-original-async','p-stage-candidate','d0','d1']]
    if source_only:
        names=[RELATIVE]
    payload={name:base64.b64encode((OUT/name).read_bytes()).decode() for name in names}
    code=r'''
import ast,base64,json,subprocess,sys
from pathlib import Path
root=Path('/opt/ks-stage-ready-luv-20261027/reduced-pd');data=json.load(sys.stdin)
if root.resolve()!=root:raise ValueError('unsafe private root')
for name,blob in data.items():
 rel=Path(name);path=root/rel
 if rel.is_absolute() or '..' in rel.parts or path.is_symlink():raise ValueError('unsafe destination')
 path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(base64.b64decode(blob))
 if name.endswith('.py'):ast.parse(path.read_text())
 if name.endswith('.sh'):subprocess.run(['bash','-n',str(path)],check=True)
print(json.dumps({'files':list(data),'syntax':'PASS','service_action':False,'public_or_installed_modified':False}))
'''
    encoded=base64.b64encode(code.encode()).decode()
    cmd=('docker exec -i -e HOME='+REMOTE+'/home -e TMPDIR='+REMOTE+'/tmp ks-pp-m137-priv python -S -c '+
         shlex.quote('import base64;exec(base64.b64decode('+repr(encoded)+'))'))
    r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-i',str(Path.home()/'.ssh/id_ed25519'),
                       'root@115.120.85.137',cmd],input=json.dumps(payload).encode(),capture_output=True,timeout=75)
    if r.returncode:raise RuntimeError(r.stderr.decode(errors='replace'))
    result=json.loads(r.stdout)
    receipt_name='draft_config_n16_family_deployment.json' if source_only else 'draft_config_n16_deployment.json'
    (HERE/receipt_name).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--deploy',action='store_true')
    parser.add_argument('--source-only',action='store_true',help='refresh only the already-wired private proposer source')
    args=parser.parse_args();prepare()
    if args.deploy:
        # Root/probe must have completed the actual constructor + weight-write CPU proof.
        receipt=HERE/'draft_actual_ctor_weight_cpu.json'
        proof=json.loads(receipt.read_text(encoding='utf-8')) if receipt.is_file() else {}
        fixed=next((case for case in proof.get('cases',[]) if case.get('case')=='private_copy_fix_strict_all_seven_written'),{})
        if (proof.get('status')!='PASS_ACTUAL_AST_CONSTRUCTOR_AND_WEIGHT_WRITES_CPU_ONLY'
                or fixed.get('constructed_layers')!=['78'] or len(fixed.get('writes',[]))!=7
                or fixed.get('not_written') or fixed.get('strict_failure') is not None):
            raise RuntimeError('actual constructor/weight-write CPU PASS receipt is required before deploy')
        if args.source_only:
            family_receipt=HERE/'draft_n16_family_quant_cpu.json'
            family_proof=json.loads(family_receipt.read_text(encoding='utf-8')) if family_receipt.is_file() else {}
            if family_proof.get('status')!='PASS':
                raise RuntimeError('actual GLM-family quant-dispatch and constructor/write CPU PASS required')
        deploy(source_only=args.source_only)


if __name__=='__main__':main()
