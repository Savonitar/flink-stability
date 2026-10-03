"""Synthetic command fixtures: no real connector checkout, Maven build, network or Docker."""
from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
import connector_pr_test as tool

HEAD, BASE, OTHER = "a"*40, "b"*40, "c"*40


class SuggestionTest(unittest.TestCase):
    def test_unknown_or_empty_diff_is_full_and_known_paths_have_explicit_rules(self):
        for paths in ([], ["pom.xml"], ["new-module/X.java"]):
            self.assertEqual(["chaos-full"], tool.suggest(paths)["profiles"])
        result = tool.suggest(["flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/internal/KafkaCommitter.java"])
        self.assertEqual(["protocol", "pooling", "brokers"], result["profiles"])
        self.assertTrue(result["matches"][0]["prefix"])
        mixed = tool.suggest([result['matches'][0]['path'], "pom.xml"])
        self.assertEqual(["chaos-full"], mixed["profiles"])


class SyntheticCommandTest(unittest.TestCase):
    def setUp(self):
        # The ignored runtime-output directory is absent in clean checkouts.
        (ROOT / 'jobs').mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=ROOT / 'jobs', prefix='connector-tool-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.checkout = self.root / 'connector'
        (self.checkout / '.git').mkdir(parents=True)
        self.output = self.root / 'output'
        self.worktrees = {}
        self.calls = []
        self.fail_build = False
        self.gate_exit = 0
        self.dirty = False
        self.tree_entries = '100644 blob ' + 'd'*40 + '\tflink-connector-kafka/pom.xml'
        self.paths = 'flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/KafkaSource.java\0'

    def git_output(self, argv, **kwargs):
        self.calls.append(argv)
        self.assertEqual('git', argv[0])
        checkout, args = Path(argv[2]), argv[3:]
        if args == ['rev-parse', '--show-toplevel']: return str(self.checkout)+'\n'
        if args[:2] == ['rev-parse', '--verify']:
            ref = args[-1].removesuffix('^{commit}')
            return self.worktrees.get(checkout, HEAD) if ref == 'HEAD' else {'head':HEAD,'base':BASE,'main':OTHER}.get(ref,ref)
        if args[:1] == ['merge-base']: return BASE
        if args[:1] == ['ls-tree']: return self.tree_entries
        if args[:1] == ['status']: return ''
        if args[:1] == ['rev-parse'] and args[-1].endswith('^{tree}'): return 'e'*40
        if args[:1] == ['diff']:
            return self.paths if '-z' in args else ('changed.java' if self.dirty else '')
        raise AssertionError(argv)

    def fake_run(self, argv, **kwargs):
        argv = list(map(str,argv)); self.calls.append(argv)
        log = kwargs.get('stdout')
        if argv[0] == 'git':
            path = Path(argv[-2]); self.worktrees[path] = argv[-1]
            (path / 'flink-connector-kafka').mkdir(parents=True)
            (path / 'pom.xml').write_text('<project/>')
            (path / 'flink-connector-kafka/pom.xml').write_text('<project/>')
        elif argv[0] == sys.executable:
            self.assertTrue(argv[1].endswith('tools/pr_gate.py'))
            self.assertIn('--baseline-connector-jar', argv)
            self.assertIn('--baseline-runtime-dir', argv)
            return subprocess.CompletedProcess(argv,self.gate_exit)
        elif '-version' in argv:
            log.write('Synthetic Maven/JDK version\n')
        elif 'install' in argv:
            if self.fail_build: return subprocess.CompletedProcess(argv,1)
            module = Path(argv[argv.index('-f')+1]).parent / 'flink-connector-kafka'
            (module / 'target').mkdir()
            (module / 'target/flink-connector-kafka-1.jar').write_bytes(str(module).encode())
            (module / 'target/flink-connector-kafka-1-tests.jar').write_bytes(b'excluded')
        elif tool.DEPENDENCY_PLUGIN in argv:
            directory = Path(next(x.split('=',1)[1] for x in argv if x.startswith('-DoutputDirectory=')))
            (directory / 'kafka-clients.jar').write_bytes(b'synthetic dependency')
        else: raise AssertionError(argv)
        return subprocess.CompletedProcess(argv,0)

    def invoke(self, *extra):
        argv = ['tool','--checkout',str(self.checkout),'--head','head','--base','base','--output',str(self.output),*extra]
        with patch.object(sys,'argv',argv),patch.object(tool.Path,'cwd',return_value=self.root), \
             patch.object(tool.subprocess,'check_output',side_effect=self.git_output), \
             patch.object(tool.subprocess,'run',side_effect=self.fake_run),redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO()):
            return tool.main()

    def test_builds_isolated_refs_records_provenance_and_passes_explicit_base(self):
        self.assertEqual(0,self.invoke('--profile','brokers'))
        manifest=json.loads((self.output/'manifest.json').read_text())
        self.assertEqual({HEAD,BASE},{v['commit'] for v in manifest['builds'].values()})
        self.assertEqual(2,len(self.worktrees))
        for side,build in manifest['builds'].items():
            self.assertTrue(Path(build['worktree']).is_relative_to(self.output/side))
            self.assertEqual(64,len(build['jarSha256']))
            self.assertEqual('e'*40,build['tree'])
            self.assertEqual(1,len(build['runtimeSha256']))
        builds=[c for c in self.calls if 'install' in c]
        self.assertNotEqual(next(x for x in builds[0] if x.startswith('-Dmaven.repo.local=')),next(x for x in builds[1] if x.startswith('-Dmaven.repo.local=')))
        self.assertTrue(all('-s' in c and '-gs' in c for c in builds))
        self.assertIn('<mirrorOf>*</mirrorOf>',(self.output/'central-settings.xml').read_text())
        self.assertIn('Synthetic',manifest['mavenVersion']);self.assertIn('Synthetic',manifest['jdkVersion'])
        self.assertEqual(BASE,manifest['plan']['baselineCommit'])
        self.assertFalse(any('fetch' in c or 'push' in c or 'remove' in c for c in self.calls))

    def test_merge_base_and_dry_run_do_not_create_worktrees_or_run_maven(self):
        argv=['tool','--checkout',str(self.checkout),'--head','head','--merge-base-of','main','--output',str(self.output),'--dry-run']
        with patch.object(sys,'argv',argv),patch.object(tool.Path,'cwd',return_value=self.root),patch.object(tool.subprocess,'check_output',side_effect=self.git_output),patch.object(tool.subprocess,'run',side_effect=AssertionError('mutation')),redirect_stdout(io.StringIO()):
            self.assertEqual(0,tool.main())
        self.assertFalse(self.output.exists())
        self.assertTrue(any('merge-base' in c for c in self.calls))

    def test_failures_keep_evidence_and_never_run_gate(self):
        self.fail_build=True
        self.assertEqual(2,self.invoke())
        self.assertTrue((self.output/'failure.json').is_file())
        self.assertTrue((self.output/'baseline/worktree').is_dir())
        self.assertFalse(any(c[0]==sys.executable for c in self.calls))

    def test_existing_output_is_never_modified(self):
        self.output.mkdir();(self.output/'plan.json').write_text('original')
        self.assertEqual(2,self.invoke())
        self.assertEqual(['plan.json'],[p.name for p in self.output.iterdir()])
        self.assertEqual('original',(self.output/'plan.json').read_text())

    def test_relative_maven_path_is_bound_before_build_working_directories_change(self):
        with patch.object(tool.Path, 'absolute', autospec=True, side_effect=lambda path: path if path.is_absolute() else self.root / path):
            self.assertEqual(0, self.invoke('--maven', './build-tools/mvn', '--build-only'))
        builds = [call for call in self.calls if 'install' in call or tool.DEPENDENCY_PLUGIN in call]
        self.assertEqual(4, len(builds))
        self.assertTrue(all(call[0] == str(self.root / 'build-tools/mvn') for call in builds))

    def test_unsafe_project_overrides_external_paths_and_changed_sources_fail_closed(self):
        self.tree_entries='100644 blob '+'d'*40+'\t.mvn/maven.config'
        self.assertEqual(2,self.invoke());self.assertFalse(self.output.exists())
        self.tree_entries='120000 blob '+'d'*40+'\toutside'
        self.assertEqual(2,self.invoke());self.assertFalse(self.output.exists())
        with self.assertRaises(ValueError):tool.inside(self.root,self.root/'../outside')
        self.tree_entries='100644 blob '+'d'*40+'\tflink-connector-kafka/pom.xml'
        self.dirty=True
        self.assertEqual(2,self.invoke());self.assertFalse(any(c[0]==sys.executable for c in self.calls))

    def test_build_only_and_gate_failure_exit_are_preserved(self):
        self.assertEqual(0,self.invoke('--build-only'))
        self.assertFalse(any(c[0]==sys.executable for c in self.calls))
        self.output=self.root/'second-output';self.gate_exit=1
        self.assertEqual(1,self.invoke())
