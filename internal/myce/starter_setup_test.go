package myce

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPyTorchSetupPinnedCUDAAndBackendFailure(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	setup, err := filepath.Abs("../../extensions/pytorch-script/setup.py")
	if err != nil {
		t.Fatal(err)
	}
	code := `import os,runpy,sys,tempfile,subprocess
from pathlib import Path
setup=sys.argv[1]
with tempfile.TemporaryDirectory() as folder:
 cache=Path(folder);(cache/'venv/bin').mkdir(parents=True);(cache/'venv/bin/python').touch()
 os.environ.update(MYCE_EXTENSION_CACHE=folder,MYCE_EXTENSION_DIR=str(Path(setup).parent),MYCE_PLATFORM='cuda')
 sys.platform='linux'
 calls=[]
 def run(argv,check):
  calls.append(argv)
  if argv[-1].endswith('backend.py'):raise subprocess.CalledProcessError(1,argv)
 subprocess.run=run
 try:runpy.run_path(setup,run_name='__main__')
 except subprocess.CalledProcessError:pass
 else:raise AssertionError('backend failure was swallowed')
 assert len(calls)==2,calls
 assert 'torch==2.9.1+cu130' in calls[0],calls
 assert calls[0][-2:]==['--index-url','https://download.pytorch.org/whl/cu130'],calls
`
	if out, err := exec.Command(python, "-c", code, setup).CombinedOutput(); err != nil {
		t.Fatalf("setup contract: %v\n%s", err, out)
	}
}
