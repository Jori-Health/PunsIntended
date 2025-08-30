.PHONY: stage3 query
stage3:
	python3 -m stageA run artifacts/bm25 artifacts/dense data_lake/chunks/*/chunks.jsonl "progression after FOLFIRINOX" .stage3/A && \
	python3 -m stageB run .stage3/A/candidates.jsonl data_lake/chunks/*/chunks.jsonl .stage3/B && \
	python3 -m stageC run .stage3/B/rescored.jsonl data_lake/chunks/*/chunks.jsonl .stage3/C

query:
	@[ -n "$$Q" ] || (echo 'Usage: make query Q="your query"'; exit 1)
	python3 -m stageA run artifacts/bm25 artifacts/dense data_lake/chunks/*/chunks.jsonl "$$Q" .stage3/A && \
	python3 -m stageB run .stage3/A/candidates.jsonl data_lake/chunks/*/chunks.jsonl .stage3/B && \
	python3 -m stageC run .stage3/B/rescored.jsonl data_lake/chunks/*/chunks.jsonl .stage3/C && \
	head -n 10 .stage3/C/final.jsonl

