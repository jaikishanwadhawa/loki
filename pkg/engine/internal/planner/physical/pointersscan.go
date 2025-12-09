package physical

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// TODO: find a better naming

type PointersScan struct {
	NodeID ulid.ULID

	Location DataObjLocation

	Selector Expression

	MaxTimeRange TimeRange
	Start        time.Time
	End          time.Time
}

func (s *PointersScan) ID() ulid.ULID { return s.NodeID }

func (s *PointersScan) Clone() Node {
	return &PointersScan{
		NodeID:       ulid.Make(),
		Location:     s.Location,
		Selector:     cloneExpression(s.Selector),
		MaxTimeRange: s.MaxTimeRange,
		Start:        s.Start,
		End:          s.End,
	}
}

func (s *PointersScan) Type() NodeType {
	return NodeTypePointersScan
}
