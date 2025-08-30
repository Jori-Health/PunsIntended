import subprocess, sys, pathlib, json, os

def test_stage3_pipeline(tmp_path):
    repo = pathlib.Path(__file__).resolve().parents[1]
    bm25 = repo / "artifacts" / "bm25"
    dense = repo / "artifacts" / "dense"
    chunks = repo / "data_lake" / "chunks"
    outA = tmp_path / "A"; outA.mkdir()
    outB = tmp_path / "B"; outB.mkdir()
    outC = tmp_path / "C"; outC.mkdir()
    q = "progression after FOLFIRINOX"

    # Stage A
    rA = subprocess.run([sys.executable,"-m","stageA","run",str(bm25),str(dense),str(chunks.glob("**/chunks.jsonl").__iter__().__next__()),q,str(outA)],capture_output=True,text=True)
    assert rA.returncode==0

    # Stage B
    rB = subprocess.run([sys.executable,"-m","stageB","run",str(outA/"candidates.jsonl"),str(chunks.glob("**/chunks.jsonl").__iter__().__next__()),str(outB)],capture_output=True,text=True)
    assert rB.returncode==0

    # Stage C
    rC = subprocess.run([sys.executable,"-m","stageC","run",str(outB/"rescored.jsonl"),str(chunks.glob("**/chunks.jsonl").__iter__().__next__()),str(outC)],capture_output=True,text=True)
    assert rC.returncode==0

    final = outC / "final.jsonl"
    assert final.exists()
    lines = final.read_text().strip().splitlines()
    assert 1 <= len(lines) <= 10

