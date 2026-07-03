package replay

import (
	"fmt"

	"quorum/internal/model"
)

// Replay folds committed findings into per-doc versions, verifying that each
// finding's BaseVersion matches the running version (I2: no lost updates;
// I5: no gaps). Findings must be supplied in append (Seq) order.
func Replay(findings []model.Finding) (map[string]int, error) {
	versions := map[string]int{}
	var prevSeq int64
	for _, f := range findings {
		if f.Seq <= prevSeq {
			return nil, fmt.Errorf("out-of-order or duplicate seq %d (previous %d)", f.Seq, prevSeq)
		}
		prevSeq = f.Seq
		if f.BaseVersion != versions[f.DocID] {
			return nil, fmt.Errorf("doc %s seq %d: base_version %d != running %d",
				f.DocID, f.Seq, f.BaseVersion, versions[f.DocID])
		}
		if f.CommittedVersion != versions[f.DocID]+1 {
			return nil, fmt.Errorf("doc %s seq %d: committed_version %d != running+1 %d",
				f.DocID, f.Seq, f.CommittedVersion, versions[f.DocID]+1)
		}
		versions[f.DocID] = f.CommittedVersion
	}
	return versions, nil
}
