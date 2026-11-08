"""Source-identical collector preparation extracted from the verified native probe."""
from build_gprofng_timer_probe import (ARCHIVE_URL,ARCHIVE_SHA,ROOT,source_members,patch,
    atomic_json,sha,require,safe_executable,build_command,run)
import shutil,sys,tarfile,urllib.request

def build_collector(work,env,budget,records):
    archive=work/'source.tar.xz'
    with urllib.request.urlopen(ARCHIVE_URL,timeout=30) as response, archive.open('xb') as out:
        total=0
        while block:=response.read(1024*1024):
            total+=len(block);require(total<=32_000_000,'download_bound');out.write(block);budget.check()
    require(sha(archive)==ARCHIVE_SHA,'archive_identity')
    source=work/'source'
    with tarfile.open(archive) as tar:
        for member,relative in source_members(tar):
            path=source/relative;path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
            with tar.extractfile(member) as inp,path.open('xb') as out:shutil.copyfileobj(inp,out)
            path.chmod(0o700 if member.mode&0o111 else 0o600)
    budget.check();archive.unlink()
    dispatcher=source/'gprofng/libcollector/dispatcher.c'
    dispatcher.write_bytes(patch(dispatcher.read_bytes()))
    atomic_json(work/'source.json',dict(archive_sha256=ARCHIVE_SHA,patched_dispatcher=sha(dispatcher),
        extracted_files=1876,extracted_bytes=98250115))
    shell=safe_executable(shutil.which('sh',path=os.defpath))
    make=safe_executable(shutil.which('make',path=os.defpath))
    cc=safe_executable(shutil.which('cc',path=os.defpath))
    tool=safe_executable(shutil.which('gprofng',path=os.defpath))
    tracer=safe_executable(shutil.which('strace',path=os.defpath))
    atomic_json(work/'tools.json',{name:dict(path=str(path),sha256=sha(path)) for name,path in
        [('sh',shell),('make',make),('cc',cc),('gprofng',tool),('strace',tracer)]})
    for name in ('bfd','gprofng'): (work/'build'/name).mkdir(mode=0o700)
    bfd=work/'build/bfd';gp=work/'build/gprofng'
    build_command([str(shell),str(source/'bfd/configure'),'--disable-nls','--disable-werror'],
        'bfd-configure',bfd,work,env,budget,records)
    build_command([str(make),'-j2','stmp-bfd-h'],'bfd-headers',bfd,work,env,budget,records)
    build_command([str(shell),str(source/'gprofng/configure'),'--disable-gprofng-tools','--disable-werror','--disable-nls'],
        'collector-configure',gp,work,env,budget,records)
    build_command([str(make),'-C',str(gp),'-j2','all'],
        'collector-build',gp,work,env,budget,records)
    build_command([str(cc),'-std=c11','-O1','-g0','-Wall','-Wextra','-Werror',
        str(ROOT/'scripts/ops/gprofng_timer_validity_test.c'),'-o',str(work/'timer-test')],
        'timer-test-build',work,work,env,budget,records)
    require(run([str(work/'timer-test')],'timer-unit',work,env,budget,records)==0,'timer_unit')
    assets=work/'assets'
    for name in ('libgp-collector.so','libgp-collectorAPI.so'):
        file=gp/'libcollector/.libs'/name
        require(file.is_file() and file.stat().st_size<32_000_000,'library_bound')
        shutil.copyfile(file,assets/name);(assets/name).chmod(0o500)
    library=assets/'libgp-collector.so'
    atomic_json(work/'asset-manifest.json',{p.name:sha(p) for p in assets.iterdir()})
    return assets
