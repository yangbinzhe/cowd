import argparse,csv,os,re,subprocess,time,json,hashlib
from collections import Counter
from pathlib import Path
def classify(text, rc):
 matches = re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)
 a, b, c = map(int, matches[-1]) if matches else (0, 0, 0)
 selected = [int(n) for n in re.findall(r'^running (\d+) tests?$', text, re.MULTILINE)]
 if 'COWD_TEST_SKIPPED' in text:
  return 'SKIP', a, b, c
 if not matches:
  return ('STACK_ABORT' if 'stack overflow' in text else 'INVALID'), a, b, c
 if not selected or max(selected) == 0 or a + b + c == 0:
  return 'INVALID', a, b, c
 if c:
  return 'SKIP', a, b, c
 if rc == 0 and a == 1 and b == 0 and max(selected) == 1:
  return 'PASS', a, b, c
 if b == 1 and a == 0 and max(selected) == 1:
  return 'FAIL', a, b, c
 return 'INVALID', a, b, c

def main():
 parser = argparse.ArgumentParser()
 parser.add_argument('--root', type=Path, required=True)
 parser.add_argument('--out', type=Path, required=True)
 parser.add_argument('--rows', type=Path, required=True)
 parser.add_argument('--metadata', type=Path, required=True)
 args=parser.parse_args()
 root=args.root.resolve(); out=args.out.resolve(); out.mkdir(parents=True,exist_ok=False)
 rows=list(csv.DictReader(args.rows.open(),delimiter='\t'))
 metadata=json.loads(args.metadata.read_text())
 diff=subprocess.check_output(['git','-c',f'safe.directory={root}','diff','--binary','HEAD'],cwd=root);(out/'tree-diff.sha256').write_text(hashlib.sha256(diff).hexdigest()+'\n')
 untracked=subprocess.check_output(['git','-c',f'safe.directory={root}','ls-files','--others','--exclude-standard','-z'],cwd=root).split(b'\0')
 with (out/'untracked.sha256').open('w') as f:
  for x in untracked:
   if x:
    p=root/os.fsdecode(x)
    if p.is_file(): f.write(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+os.fsdecode(x)+'\n')
 with (out/'results.tsv').open('w',buffering=1) as f:
  w=csv.writer(f,delimiter='\t');w.writerow(['n','name','target','result','passed','failed','ignored','exit','log'])
  for i,r in enumerate(rows,1):
   target=r['target']; candidates=[(p['name'],t) for p in metadata['packages'] for t in p['targets'] if t['name'].replace('-','_')==target]
   if len(candidates)!=1: w.writerow([i,r['name'],target,'AMBIGUOUS',0,0,0,'-', '-']);continue
   pkg,t=candidates[0]; kind='--lib' if 'lib' in t['kind'] else '--test'; cmd=['cargo','test','--offline','--locked','-p',pkg,kind]
   if kind=='--test':cmd.append(t['name'])
   cmd.extend(['--',r['name'],'--exact','--test-threads=1','--nocapture'])
   log=out/f'{i:02d}-{target}.log';started=time.monotonic()
   try:
    with log.open('w') as lf:
     p=subprocess.run(cmd,cwd=root,stdout=lf,stderr=subprocess.STDOUT,timeout=300)
    rc=p.returncode
   except subprocess.TimeoutExpired:rc=124
   text=log.read_text(errors='replace');status,a,b,c=classify(text,rc)

   if status == 'FAIL' and target in ('architecture_dependencies','architecture_gates') and '.cowd-build/cowd-edge' in text: status='ENV_INVALID'
   w.writerow([i,r['name'],target,status,a,b,c,rc,str(log)])

 final_diff=subprocess.check_output(['git','-c',f'safe.directory={root}','diff','--binary','HEAD'],cwd=root);(out/'tree-final-diff.sha256').write_text(hashlib.sha256(final_diff).hexdigest()+'\n')

 results=list(csv.DictReader((out/'results.tsv').open(),delimiter='\t'))
 counts=Counter(row['result'] for row in results)
 summary={'total':len(results),'pass':counts['PASS'],'fail':counts['FAIL'],'skip':counts['SKIP'],'invalid':sum(v for k,v in counts.items() if k not in ('PASS','FAIL','SKIP')),'statuses':dict(counts)}
 (out/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
 return 0 if summary['pass'] == summary['total'] and summary['total'] else 1

if __name__ == '__main__':
 raise SystemExit(main())
