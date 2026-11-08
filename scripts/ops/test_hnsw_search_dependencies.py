"""A one-module wheel reproduces pip's explicit bytecode compilation."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

from hnsw_search_dependencies import install_wheels


class DependencyTests(unittest.TestCase):
    def test_offline_install_preserves_module_without_generating_bytecode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            wheel=root/'hnsw_compile_probe-1.0-py3-none-any.whl'
            files={'hnsw_compile_probe.py':'VALUE = 42\n',
                'hnsw_compile_probe-1.0.dist-info/METADATA':
                    'Metadata-Version: 2.1\nName: hnsw-compile-probe\nVersion: 1.0\n',
                'hnsw_compile_probe-1.0.dist-info/WHEEL':
                    'Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n'}
            files['hnsw_compile_probe-1.0.dist-info/RECORD']=''.join(f'{name},,\n' for name in files)+'hnsw_compile_probe-1.0.dist-info/RECORD,,\n'
            with zipfile.ZipFile(wheel,'w') as archive:
                for name,content in files.items():archive.writestr(name,content)
            env={key:value for key,value in os.environ.items() if not key.startswith('PIP_')}
            env.update(PYTHONDONTWRITEBYTECODE='1',PIP_CONFIG_FILE=os.devnull,
                PIP_DISABLE_PIP_VERSION_CHECK='1',TMPDIR=str(root))
            # The previous -B-only command still explicitly compiled the module.
            control=subprocess.run([sys.executable,'-B','-m','pip','install','--no-index','--no-deps',
                '--no-cache-dir','--target',str(root/'control'),str(wheel)],
                env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
            self.assertEqual(control.returncode,0,control.stderr.decode())
            self.assertTrue(list((root/'control').rglob('*.pyc')))
            class Budget:
                def run(self,command,*,timeout,log,env):
                    with log.open('ab') as output:
                        subprocess.run([*command,'--target',str(root/'installed')],env=env,
                            stdout=output,stderr=subprocess.STDOUT,check=True,timeout=timeout)
            install_wheels(sys.executable,[wheel],root,Budget(),env)
            module=root/'installed/hnsw_compile_probe.py'
            self.assertEqual(module.read_text(),files['hnsw_compile_probe.py'])
            subprocess.run([sys.executable,'-B','-c',
                'import sys;sys.path.insert(0,sys.argv[1]);import hnsw_compile_probe;assert hnsw_compile_probe.VALUE==42',
                str(root/'installed')],env=env,check=True,timeout=10)
            self.assertFalse(list((root/'installed').rglob('*.pyc')))
            self.assertFalse(list((root/'installed').rglob('__pycache__')))


if __name__=='__main__':unittest.main()
