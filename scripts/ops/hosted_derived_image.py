"""Build an offline derived image inside the existing managed VM boundary."""
import base64
import gzip
import json
import os
from pathlib import Path
import sys

from hosted_native_image import construct


def main():
    if os.environ.get('RUNNER_KIND')!='github-hosted' or os.geteuid()!=0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    try:
        os.umask(0o077)
        repo=Path(os.environ['GITHUB_WORKSPACE']).resolve()
        temporary=Path(os.environ['RUNNER_TEMP']).resolve()
        inputs=json.loads(gzip.decompress(base64.b64decode(os.environ.pop('NATIVE_IMAGE_INPUTS'),validate=True)))
        config=json.loads(base64.b64decode(os.environ.pop('PUBLIC_DERIVED_IMAGE_CONFIG'),validate=True))
        assert 'image_archive' not in inputs and 'derived_image' not in inputs
        inputs['derived_image']=config['base_image']
        result=construct(repo,temporary/'alopex-native-private',repo/'native-export',
                         inputs,repo/'.github/ops/native-evidence-cert.pem')
        print(json.dumps({'status':'passed' if result==0 else 'failed','error_code':None if result==0 else 'CONSTRUCTION'}))
        return result
    except BaseException:
        print('{"status":"failed","error_code":"CONSTRUCTION"}')
        return 1


if __name__=='__main__':raise SystemExit(main())
