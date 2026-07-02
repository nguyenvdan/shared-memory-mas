package corpus

import (
	"bufio"
	"encoding/json"
	"os"

	"quorum/internal/model"
)

// Load parses a JSONL corpus file into Docs. Blank lines are skipped.
func Load(path string) ([]model.Doc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var docs []model.Doc
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20) // 1 MiB lines
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var d model.Doc
		if err := json.Unmarshal(line, &d); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, sc.Err()
}
