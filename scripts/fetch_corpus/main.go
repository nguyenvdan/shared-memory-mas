package main

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"quorum/internal/model"
)

// Minimal Atom parsing for the arXiv API response.
type feed struct {
	Entries []struct {
		ID       string `xml:"id"`
		Title    string `xml:"title"`
		Summary  string `xml:"summary"`
		Category []struct {
			Term string `xml:"term,attr"`
		} `xml:"category"`
	} `xml:"entry"`
}

func main() {
	cats := flag.String("categories", "cs.DC,cs.OS", "comma-separated arXiv categories")
	max := flag.Int("max", 300, "number of abstracts to fetch")
	out := flag.String("out", "corpus/arxiv.jsonl", "output JSONL path")
	flag.Parse()

	query := "cat:" + strings.Join(strings.Split(*cats, ","), "+OR+cat:")
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	const page = 100
	hasher := sha256.New()
	w := io.MultiWriter(f, hasher)
	written := 0

	for start := 0; start < *max; start += page {
		n := page
		if start+n > *max {
			n = *max - start
		}
		u := fmt.Sprintf(
			"https://export.arxiv.org/api/query?search_query=%s&start=%d&max_results=%d&sortBy=submittedDate&sortOrder=descending",
			query, start, n,
		)
		resp, err := http.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var fd feed
		if err := xml.Unmarshal(body, &fd); err != nil {
			log.Fatalf("parse: %v", err)
		}
		for _, e := range fd.Entries {
			doc := model.Doc{
				ID:       strings.TrimSpace(e.ID),
				Title:    strings.Join(strings.Fields(e.Title), " "),
				Abstract: strings.Join(strings.Fields(e.Summary), " "),
			}
			for _, c := range e.Category {
				doc.Categories = append(doc.Categories, c.Term)
			}
			line, _ := json.Marshal(doc)
			w.Write(append(line, '\n'))
			written++
		}
		time.Sleep(3 * time.Second) // arXiv rate-limit etiquette
	}

	manifest := fmt.Sprintf("file: %s\ndocs: %d\nsha256: %x\n", *out, written, hasher.Sum(nil))
	if err := os.WriteFile("corpus/MANIFEST", []byte(manifest), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %d docs to %s", written, *out)
}
