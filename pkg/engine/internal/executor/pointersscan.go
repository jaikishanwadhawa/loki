package executor

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/go-kit/log"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/pointers"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/grafana/loki/v3/pkg/xcap"
	"github.com/pkg/errors"
)

type pointersScanOptions struct {
	StreamIDs        []int64
	StreamsSection   *streams.Section
	PointersSections []*pointers.Section

	Start time.Time
	End   time.Time

	Selector physical.Expression

	BatchSize int64 // The buffer size for reading rows, derived from the engine batch size.
}

type pointersScan struct {
	opts   pointersScanOptions
	logger log.Logger
	region *xcap.Region

	initialized       bool
	matchingStreamIDs []scalar.Scalar

	pointersSectionIdx int
	currentCols        []*pointers.Column
	currentReader      *pointers.Reader

	sStart *scalar.Timestamp
	sEnd   *scalar.Timestamp
}

var _ Pipeline = (*pointersScan)(nil)

func newPointersScanPipeline(opts pointersScanOptions, logger log.Logger, region *xcap.Region) *pointersScan {
	return &pointersScan{
		opts:               opts,
		logger:             logger,
		region:             region,
		sStart:             scalar.NewTimestampScalar(arrow.Timestamp(opts.Start.UnixNano()), arrow.FixedWidthTypes.Timestamp_ns),
		sEnd:               scalar.NewTimestampScalar(arrow.Timestamp(opts.End.UnixNano()), arrow.FixedWidthTypes.Timestamp_ns),
		pointersSectionIdx: -1,
	}
}

func (s *pointersScan) Read(ctx context.Context) (arrow.RecordBatch, error) {
	if err := s.init(ctx); err != nil {
		return nil, err
	}

	ctx = xcap.ContextWithRegion(ctx, s.region)
	for {
		rec, err := s.read(ctx)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		} else if (rec == nil || rec.NumRows() == 0) && errors.Is(err, io.EOF) {
			// the section is fully read, proceed to the next one, continue the iteration so we read from the next section
			err := s.prepareForNextSection()
			if err != nil {
				return nil, err
			}
			continue
		}

		return rec, nil
	}
}

// read reads the entire section into memory and generates an [arrow.RecordBatch]
// from the data. It returns an error if reading a section resulted in an
// error.
func (s *pointersScan) read(ctx context.Context) (arrow.RecordBatch, error) {
	rec, err := s.currentReader.Read(ctx, int(s.opts.BatchSize))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	} else if (rec == nil || rec.NumRows() == 0) && errors.Is(err, io.EOF) {
		return nil, EOF
	}

	return rec, nil
}

func (s *pointersScan) init(ctx context.Context) error {
	if s.initialized {
		return nil
	}

	if len(s.opts.PointersSections) == 0 {
		return EOF
	}

	// find stream ids that satisfy the predicate and start/end
	var err error
	s.matchingStreamIDs, err = s.findMatchingStreamIDs(ctx)
	if err != nil {
		return fmt.Errorf("creating matching stream ids: %w", err)
	}

	if s.matchingStreamIDs == nil {
		return EOF
	}

	err = s.prepareForNextSection()
	if err != nil {
		return err
	}

	s.initialized = true

	return nil
}

func (s *pointersScan) findMatchingStreamIDs(ctx context.Context) ([]scalar.Scalar, error) {
	//TODO(ivkalita): implement using columnar reader, the main complexity is in predicate mapping
	var result []scalar.Scalar

	var reader streams.RowReader
	defer reader.Close()

	p, err := buildStreamsPredicate(s.opts.Selector, s.sStart.ToTime(), s.sEnd.ToTime())
	if err != nil {
		return nil, fmt.Errorf("building streams predicate: %w", err)
	}

	reader.Reset(s.opts.StreamsSection)
	if p != nil {
		err := reader.SetPredicate(p)
		if err != nil {
			return nil, fmt.Errorf("setting stream predicate: %w", err)
		}
	}

	buf := make([]streams.Stream, 1024)

	for {
		num, err := reader.Read(ctx, buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if num == 0 && errors.Is(err, io.EOF) {
			break
		}
		for _, stream := range buf[:num] {
			result = append(result, scalar.NewInt64Scalar(stream.ID))
		}
	}

	return result, nil
}

func (s *pointersScan) prepareForNextSection() error {
	for {
		skip, err := s.prepareForNextSectionOrSkip()
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		return nil
	}
}

func (s *pointersScan) prepareForNextSectionOrSkip() (bool, error) {
	s.pointersSectionIdx++
	if s.pointersSectionIdx == len(s.opts.PointersSections) {
		// no more pointers sections to read
		return false, EOF
	}

	if s.currentReader != nil {
		_ = s.currentReader.Close()
	}

	curSection := s.opts.PointersSections[s.pointersSectionIdx]
	cols, err := s.findColumnsByType(
		curSection.Columns(),
		pointers.ColumnTypePath,
		pointers.ColumnTypeSection,
		pointers.ColumnTypeStreamID,
		pointers.ColumnTypeStreamIDRef,
		pointers.ColumnTypeMinTimestamp,
		pointers.ColumnTypeMaxTimestamp,
		pointers.ColumnTypeRowCount,
		pointers.ColumnTypeUncompressedSize,
	)
	if err != nil {
		return false, fmt.Errorf("finding pointers columns: %w", err)
	}

	var (
		colStreamID     *pointers.Column
		colMinTimestamp *pointers.Column
		colMaxTimestamp *pointers.Column
	)

	for _, c := range cols {
		if c.Type == pointers.ColumnTypeStreamID {
			colStreamID = c
		}
		if c.Type == pointers.ColumnTypeMinTimestamp {
			colMinTimestamp = c
		}
		if c.Type == pointers.ColumnTypeMaxTimestamp {
			colMaxTimestamp = c
		}
		if colStreamID != nil && colMinTimestamp != nil && colMaxTimestamp != nil {
			break
		}
	}

	if colStreamID == nil || colMinTimestamp == nil || colMaxTimestamp == nil {
		// the section has no rows with stream-based indices and can be ignored completely
		return true, nil
	}

	s.currentCols = cols

	s.currentReader = pointers.NewReader(pointers.ReaderOptions{
		Columns: s.currentCols,
		Predicates: []pointers.Predicate{
			pointers.WhereTimeRangeOverlapsWith(colMinTimestamp, colMaxTimestamp, s.sStart, s.sEnd),
			pointers.InPredicate{
				Column: colStreamID,
				Values: s.matchingStreamIDs,
			},
		},
		Allocator: memory.DefaultAllocator,
	})

	return false, nil
}

// Close closes s and releases all resources.
func (s *pointersScan) Close() {
	if s.region != nil {
		s.region.End()
	}
	if s.currentReader != nil {
		_ = s.currentReader.Close()
	}

	s.initialized = false
	s.currentReader = nil
	s.region = nil
	s.matchingStreamIDs = nil
	s.pointersSectionIdx = -1
}

func (s *pointersScan) findColumnsByType(allColumns []*pointers.Column, columnTypes ...pointers.ColumnType) ([]*pointers.Column, error) {
	result := make([]*pointers.Column, 0, len(columnTypes))

	for _, c := range allColumns {
		for _, neededType := range columnTypes {
			if neededType != c.Type {
				continue
			}

			result = append(result, c)
		}
	}

	return result, nil
}
