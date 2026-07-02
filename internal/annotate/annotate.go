package annotate

import (
	"sort"
	"strings"

	"quorum/internal/model"
)

var stopwords = map[string]bool{
	"the": true, "and": true, "for": true, "are": true, "was": true,
	"with": true, "that": true, "this": true, "from": true, "our": true,
	"its": true, "into": true, "over": true, "such": true, "these": true,
}

// Annotate returns a deterministic annotation: the top-k keywords of the
// abstract by frequency, tie-broken alphabetically, space-joined.
func Annotate(doc model.Doc, k int) string {
	counts := map[string]int{}
	for _, tok := range tokenize(doc.Abstract) {
		if len(tok) < 3 || stopwords[tok] {
			continue
		}
		counts[tok]++
	}
	type kv struct {
		word  string
		count int
	}
	kvs := make([]kv, 0, len(counts))
	for w, c := range counts {
		kvs = append(kvs, kv{w, c})
	}
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].count != kvs[j].count {
			return kvs[i].count > kvs[j].count
		}
		return kvs[i].word < kvs[j].word
	})
	top := make([]string, 0, k)
	for i := 0; i < len(kvs) && i < k; i++ {
		top = append(top, kvs[i].word)
	}
	return strings.Join(top, " ")
}

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
}
