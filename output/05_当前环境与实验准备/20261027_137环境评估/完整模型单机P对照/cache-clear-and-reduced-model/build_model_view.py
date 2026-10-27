"""Build a private layer-reduced view; default mode only reads metadata.

No framework import, NPU access, tensor payload read, or large weight copy.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import re
import struct

PRIVATE_ROOT = Path('/opt/ks-stage-ready-luv-20261027/reduced-pd')
ORIGINAL = Path('/opt/ks-models/GLM-5.1-w8a8c8-MTP')
INDEX_NAME = 'quant_model_weights.safetensors.index.json'
MTP_SUFFIXES = ('enorm.weight', 'hnorm.weight', 'shared_head.norm.weight',
                'shared_head.head.weight', 'eh_proj.weight', 'embed_tokens.weight')
METADATA_LINKS = ('chat_template.jinja', 'configuration.json', 'generation_config.json',
                  'quant_model_description.json', 'tokenizer.json', 'tokenizer_config.json')


def layer_of(name):
    match = re.match(r'^model\.layers\.(\d+)\.', name)
    return int(match.group(1)) if match else None


def inspect(source, destination, layers):
    source = source.resolve(strict=True)
    if source != ORIGINAL.resolve(strict=True):
        raise ValueError('source must be the confirmed original model')
    destination = destination.absolute()
    if (destination.parent.resolve() != PRIVATE_ROOT.resolve()
            or destination.name != f'model-N{layers}'
            or destination.exists() or destination.is_symlink()):
        raise ValueError('destination must be a new direct private model-N directory')
    config = json.loads((source/'config.json').read_text(encoding='utf-8'))
    original_layers = config['num_hidden_layers']
    if not 4 <= layers < original_layers or config.get('num_nextn_predict_layers') != 1:
        raise ValueError('unsupported layer count or original MTP configuration')
    index = json.loads((source/INDEX_NAME).read_text(encoding='utf-8'))
    weight_map = index['weight_map']
    required = {k:v for k,v in weight_map.items()
                if layer_of(k) is None or layer_of(k) < layers}
    shards = sorted(set(required.values()))
    visible = {k:v for k,v in weight_map.items() if v in shards}
    spill = [k for k in visible if layer_of(k) is not None and layer_of(k) > layers]
    if spill:
        raise ValueError(f'selected shards expose unsupported layers: {spill[:5]}')
    missing_mtp = [f'model.layers.{original_layers}.{suffix}' for suffix in MTP_SUFFIXES
                   if f'model.layers.{original_layers}.{suffix}' not in weight_map]
    if missing_mtp:
        raise ValueError(f'original draft checkpoint is missing MTP names: {missing_mtp}')
    header_read_bytes = 0
    tensor_bytes = 0
    for shard in shards:
        path = source/shard
        if Path(shard).name != shard or not path.is_file():
            raise ValueError(f'unsafe or missing shard: {shard}')
        with path.open('rb') as stream:
            raw = stream.read(8)
            if len(raw) != 8:
                raise ValueError(f'invalid safetensors header: {shard}')
            length = struct.unpack('<Q', raw)[0]
            if length > 64 * 1024 * 1024 or length + 8 > path.stat().st_size:
                raise ValueError(f'unexpected safetensors header size: {shard}')
            header = json.loads(stream.read(length))
        header_read_bytes += 8 + length
        header_keys = set(header)-{'__metadata__'}
        indexed_keys = {k for k,v in visible.items() if v == shard}
        if header_keys != indexed_keys:
            raise ValueError(f'index/header coverage mismatch in {shard}')
        tensor_bytes += sum(item['data_offsets'][1]-item['data_offsets'][0]
                            for key,item in header.items() if key != '__metadata__')
    for filename in METADATA_LINKS:
        if not (source/filename).is_file():
            raise ValueError(f'required model metadata is missing: {filename}')
    new_config = copy.deepcopy(config)
    new_config['num_hidden_layers'] = layers
    new_index = copy.deepcopy(index)
    new_index['weight_map'] = visible
    new_index.setdefault('metadata', {})['total_size'] = tensor_bytes
    report = {
        'source':str(source), 'destination':str(destination), 'target_layers':layers,
        'draft_model':str(source), 'draft_mtp_layer':original_layers,
        'required_weight_keys':len(required), 'visible_weight_keys':len(visible),
        'required_layers':sorted({layer_of(k) for k in required if layer_of(k) is not None}),
        'global_weight_keys':sorted(k for k in required if layer_of(k) is None),
        'boundary_layer_keys_skipped_by_official_target_loader':sum(layer_of(k)==layers for k in visible),
        'spill_keys_above_boundary':len(spill), 'shard_count':len(shards), 'shards':shards,
        'visible_tensor_bytes_from_headers':tensor_bytes,
        'safetensors_header_read_bytes':header_read_bytes, 'tensor_payload_read_bytes':0,
        'large_weight_copy_bytes':0, 'original_model_modified':False,
        'metadata_symlinks':list(METADATA_LINKS),
        'not_verified':['model construction', 'official quantized loader execution',
                        'NPU capacity', 'MTP numerical behavior', 'PD transfer'],
    }
    return destination, new_config, new_index, report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=ORIGINAL)
    parser.add_argument('--destination', type=Path, default=PRIVATE_ROOT/'model-N16')
    parser.add_argument('--layers', type=int, default=16)
    parser.add_argument('--materialize', action='store_true')
    args = parser.parse_args()
    destination, config, index, report = inspect(args.source, args.destination, args.layers)
    report['materialized'] = args.materialize
    if args.materialize:
        destination.mkdir()
        for filename, value in [('config.json', config), (INDEX_NAME, index),
                                ('private_view_manifest.json', report)]:
            with (destination/filename).open('x', encoding='utf-8', newline='\n') as stream:
                json.dump(value, stream, ensure_ascii=False, indent=2)
                stream.write('\n')
        for filename in report['shards'] + list(METADATA_LINKS):
            (destination/filename).symlink_to(args.source.resolve()/filename)
        for filename in ['config.json', INDEX_NAME, 'private_view_manifest.json']:
            (destination/filename).chmod(0o444)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
