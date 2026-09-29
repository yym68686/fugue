#!/usr/bin/env python3
"""Resolve one explicit Front configuration transition; never mutate live state."""
import argparse
import json
from pathlib import Path
import re

try:
    from . import reconcile_front_probe_transport as probe
    from . import stage_front_serving_transport as stage
    from . import handoff_front_serving_transport as handoff
except ImportError:
    import reconcile_front_probe_transport as probe
    import stage_front_serving_transport as stage
    import handoff_front_serving_transport as handoff


def validate(config):
    if set(config) != {'schema', 'generation', 'phase', 'target'} or config['schema'] != 'fugue.front-transport-transition/v1' or type(config['generation']) is not int or config['generation'] < 1:
        raise ValueError('explicit versioned Front transition required')
    phase, target = config['phase'], config['target']
    if phase not in ['observe','probe','stage','handoff'] or not isinstance(target, str):
        raise ValueError('Front transition phase or target is invalid')
    if phase == 'observe':
        if target != '': raise ValueError('observation cannot name a mutation target')
    elif phase == 'handoff':
        if not re.fullmatch(r'deploy/environments/production/front-serving-handoff/[a-z0-9]+(?:-[a-z0-9]+)*\.json', target):
            raise ValueError('public handoff must name an explicit repository declaration')
    elif not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', target) or len(target)>253:
        raise ValueError('Front transition must name one canonical declared resource')
    return config


def advance(previous, current):
    validate(current)
    if previous is None:
        if current['generation'] != 1 or current['phase'] != 'observe':
            raise ValueError('initial transition inventory must be observational')
    else:
        validate(previous)
        if current['generation'] != previous['generation']+1:
            raise ValueError('Front transition generation must advance exactly once')
    return current


def resolve(config, requested_phase, probes, staging, load_handoff):
    validate(config)
    if requested_phase not in ['probe','stage','handoff']:
        raise ValueError('a known mutation phase is required')
    if config['phase'] != requested_phase:
        return ''
    target = config['target']
    probes, staging = probe.validate(probes), stage.validate(staging)
    if probes['namespace'] != staging['namespace']:
        raise ValueError('Front declarations disagree on namespace')
    if requested_phase == 'probe':
        probe.select_named(probes,'listeners',target)
    else:
        if requested_phase == 'handoff':
            public = handoff.validate(load_handoff(target))
            if public['namespace'] != staging['namespace'] or public['expectedGeneration'] != staging['generation']:
                raise ValueError('handoff does not select the exact declared internal stage')
            target_service=public['service']
        else:
            target_service=target
        service=probe.select_named(staging,'services',target_service)['services'][0]
        listener=probe.select_named(probes,'listeners',service['probeListener'])['listeners'][0]
        if listener['observation'] != service['observation']:
            raise ValueError('Front stage and probe profiles differ')
        if requested_phase == 'handoff' and (listener['address'] != public['address'] or service['observation'] != public['observation']):
            raise ValueError('public address or profile differs from its observed candidate')
    return target


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--phase',required=True,choices=['probe','stage','handoff'])
    parser.add_argument('--previous',required=True)
    args=parser.parse_args()
    current=json.loads(Path(args.config).read_text())
    previous=json.loads(Path(args.previous).read_text())
    advance(previous,current)
    probes=json.loads(Path('deploy/environments/production/front-probe-transport/listeners.json').read_text())
    staging=json.loads(Path('deploy/environments/production/front-serving-stage/services.json').read_text())
    print(resolve(current,args.phase,probes,staging,lambda path:json.loads(Path(path).read_text())))

if __name__=='__main__':main()
