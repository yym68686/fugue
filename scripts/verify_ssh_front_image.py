import argparse
import json
import os
import re

from scripts.verify_managed_ssh_image import run
from scripts.verify_ssh_front_transport import verify as verify_transport


def validate_metadata(metadata, source, image=None):
    if metadata.get('Os')!='linux' or metadata.get('Architecture') not in {'amd64','arm64'} or metadata.get('Config',{}).get('Labels',{}).get('org.opencontainers.image.revision')!=source:
        raise ValueError('SSH front artifact provenance or architecture differs')
    if metadata.get('Config',{}).get('Entrypoint')!=['/usr/local/bin/fugue-ssh-front']:
        raise ValueError('SSH front image entrypoint differs from its contract')
    if image is not None and image not in metadata.get('RepoDigests',[]):
        raise ValueError('pulled SSH front image lacks the reviewed repository digest')
    return metadata['Architecture']


def verify(image, source):
    if not re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}',image) or not re.fullmatch(r'[a-f0-9]{40}',source):
        raise ValueError('immutable registry image and exact source required')
    run(['docker','pull',image],timeout=180)
    metadata=json.loads(run(['docker','image','inspect',image]))[0]
    architecture=validate_metadata(metadata,source,image)
    host=run(['docker','info','--format','{{.Architecture}}']).decode().strip()
    if host not in {'amd64':{'amd64','x86_64'},'arm64':{'arm64','aarch64'}}[architecture]:
        raise ValueError('front acceptance requires native image execution')
    environment=dict(os.environ,FUGUE_SSH_FRONT_TEST_IMAGE=image)
    actual=run(['go','test','./internal/sshfront','-run','^TestActualSSHFrontImage$','-count=1','-v','-timeout=2m'],timeout=150,env=environment)
    if b'--- PASS: TestActualSSHFrontImage' not in actual or b'actual front image verified' not in actual:
        raise ValueError('actual front image acceptance did not run successfully')
    transport=verify_transport()
    if transport.get('verified') is not True:
        raise ValueError('public transport handoff acceptance failed')
    return {'schema':'fugue.ssh-front-image-receipt/v1','verified':True,'image':image,'source_commit':source,'architecture':architecture,'checks':['actual-front-image','signed-route-load','persisted-exact-instance-ack','preserved-target-streams','invalid-configuration-keeps-lkg','credentials-excluded-from-urls','service-public-handoff','compensation-preserves-established-streams','graceful-shutdown-drain'],'transport':transport}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('image')
    parser.add_argument('source')
    args=parser.parse_args()
    print(json.dumps(verify(args.image,args.source),sort_keys=True))
