"""Retrieve only the pinned accepted image, using a read-only Actions token."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

from hosted_native_transfer import verify_run


def main():
    if os.environ.get('RUNNER_KIND')!='github-hosted' or os.geteuid()==0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    os.umask(0o077)
    repo=Path(os.environ['GITHUB_WORKSPACE']).resolve()
    sys.path.insert(0,str(repo/'scripts/test-env'))
    from capacity import Budget
    from common import atomic_json,digest,private_dir,require
    private=private_dir(repo/'native-fetch-private')
    try:
        config=json.loads(base64.b64decode(os.environ['PUBLIC_DERIVED_IMAGE_CONFIG'],validate=True))
        selected=config['base_image']
        roots=[repo.parent,Path(os.environ['RUNNER_TEMP'])]
        for root in (repo.parent.parent/'_actions',Path.home()/'.cargo',Path.home()/'.cache'):
            if root.exists():roots.append(root)
        budget=Budget(dict(roots=list(map(str,roots)),stop_bytes=40_000_000_000,
            reserve_bytes=config['download_reserve_bytes'],poll_seconds=2,
            planned_bytes=dict(fetch=config['download_bound_bytes'])))
        budget.before('fetch')
        private_dir(private/'tmp');os.environ['TMPDIR']=str(private/'tmp')
        def metadata(endpoint):
            return json.loads(subprocess.run(['gh','api','repos/alopex-db/alopex/'+endpoint],
                capture_output=True,text=True,check=True,timeout=30).stdout)
        verify_run(metadata('actions/runs/'+str(selected['producer_run'])),selected['producer_controller'])
        artifact=metadata('actions/artifacts/'+str(selected['artifact_id']))
        require(artifact['workflow_run']['id']==selected['producer_run'] and not artifact['expired']
            and artifact['name']=='native-test-environment' and artifact['digest']==selected['artifact_digest']
            and 0<artifact['size_in_bytes']<config['artifact_bound_bytes'],'IDENTITY')
        budget.run(['gh','run','download',str(selected['producer_run']),'--repo','alopex-db/alopex',
            '--name','native-test-environment','--dir',str(repo/'native-image-import')],timeout=300,
            log=private/'image-transfer.log')
        image=repo/selected['image_archive']
        require(image.resolve().is_relative_to(repo/'native-image-import') and image.is_file()
                and not image.is_symlink() and digest(image)==selected['image_archive_sha256'],'IDENTITY')
        public=json.loads((image.parent/'result.json').read_text())
        require(public['status']=='passed' and public['owned_resources_cleaned'] is True
            and public['controller_commit']==selected['producer_controller']
            and public['image_digest']==selected['image_record']['id'].removeprefix('sha256:')
            and public['archive_sha256']==selected['image_archive_sha256'],'IDENTITY')
        budget.check()
        print('{"status":"passed","error_code":null}')
        return 0
    except BaseException as error:
        try:
            atomic_json(private/'failure.json',{'error':repr(error)})
            export=private_dir(repo/'native-export')
            subprocess.run(['openssl','cms','-encrypt','-aes-256-cbc','-binary','-outform','DER',
                '-in',str(private/'failure.json'),'-out',str(export/'transfer-failure.cms'),
                str(repo/'.github/ops/native-evidence-cert.pem')],capture_output=True,check=True,timeout=30)
        except BaseException:
            pass
        print('{"status":"failed","error_code":"TRANSFER"}')
        return 1


if __name__=='__main__':raise SystemExit(main())
