package physical

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/engine/internal/util/dag"
	"github.com/oklog/ulid/v2"
)

// MetastoreRequestKind describes what metadata the planner needs.
type MetastoreRequestKind uint8

const (
	// MetastoreRequestKindSections requests section descriptors (current behavior).
	MetastoreRequestKindSections MetastoreRequestKind = iota
)

// MetastoreRequest captures the information required to resolve metadata prior
// to physical planning.
type MetastoreRequest struct {
	Kind MetastoreRequestKind

	Selector   Expression
	Predicates []Expression

	Shard ShardInfo

	From    time.Time
	Through time.Time
}

// MetastoreResponse contains the payload returned by a metastore execution.
type MetastoreResponse struct {
	Kind MetastoreRequestKind

	Sections []*metastore.DataobjSectionDescriptor
}

type metaqueryRequestKey struct {
	kind       MetastoreRequestKind
	selector   string
	predicates string
	from       int64
	through    int64
	shard      uint32
	shardOf    uint32
}

func newMetastoreRequestKey(req MetastoreRequest) metaqueryRequestKey {
	return metaqueryRequestKey{
		kind:       req.Kind,
		selector:   expressionSignature(req.Selector),
		predicates: predicatesSignature(req.Predicates),
		from:       req.From.UnixNano(),
		through:    req.Through.UnixNano(),
		shard:      req.Shard.Shard,
		shardOf:    req.Shard.Of,
	}
}

func expressionSignature(expr Expression) string {
	if expr == nil {
		return ""
	}
	return expr.String()
}

func predicatesSignature(exprs []Expression) string {
	if len(exprs) == 0 {
		return ""
	}
	parts := make([]string, len(exprs))
	for i, expr := range exprs {
		parts[i] = expressionSignature(expr)
	}
	return strings.Join(parts, ";")
}

// MetastoreCollectorCatalog records catalog lookups made during planning so
// that they can be executed as metaqueries ahead of time.
type MetastoreCollectorCatalog struct {
	requests map[metaqueryRequestKey]MetastoreRequest
}

func NewMetastoreCollectorCatalog() *MetastoreCollectorCatalog {
	return &MetastoreCollectorCatalog{
		requests: make(map[metaqueryRequestKey]MetastoreRequest),
	}
}

func (c *MetastoreCollectorCatalog) ResolveShardDescriptors(selector Expression, from, through time.Time) ([]FilteredShardDescriptor, error) {
	return c.ResolveShardDescriptorsWithShard(selector, nil, noShard, from, through)
}

func (c *MetastoreCollectorCatalog) ResolveShardDescriptorsWithShard(selector Expression, predicates []Expression, shard ShardInfo, from, through time.Time) ([]FilteredShardDescriptor, error) {
	req := MetastoreRequest{
		Kind:       MetastoreRequestKindSections,
		Selector:   cloneExpression(selector),
		Predicates: cloneExpressionSlice(predicates),
		Shard:      shard,
		From:       from,
		Through:    through,
	}

	key := newMetastoreRequestKey(req)
	if _, ok := c.requests[key]; !ok {
		c.requests[key] = req
	}
	return nil, nil
}

// Requests returns the accumulated metastore requests.
func (c *MetastoreCollectorCatalog) Requests() []MetastoreRequest {
	out := make([]MetastoreRequest, 0, len(c.requests))
	for _, req := range c.requests {
		out = append(out, req)
	}
	return out
}

// MetastorePreparedCatalog serves catalog responses for previously satisfied metaqueries.
type MetastorePreparedCatalog struct {
	responses map[metaqueryRequestKey]MetastoreResponse
}

func NewMetaqueryPreparedCatalog() *MetastorePreparedCatalog {
	return &MetastorePreparedCatalog{
		responses: make(map[metaqueryRequestKey]MetastoreResponse),
	}
}

func (c *MetastorePreparedCatalog) Store(req MetastoreRequest, resp MetastoreResponse) error {
	c.responses[newMetastoreRequestKey(req)] = resp
	return nil
}

func (c *MetastorePreparedCatalog) ResolveShardDescriptors(selector Expression, from, through time.Time) ([]FilteredShardDescriptor, error) {
	return c.ResolveShardDescriptorsWithShard(selector, nil, noShard, from, through)
}

func (c *MetastorePreparedCatalog) ResolveShardDescriptorsWithShard(selector Expression, predicates []Expression, shard ShardInfo, from, through time.Time) ([]FilteredShardDescriptor, error) {
	req := MetastoreRequest{
		Kind:       MetastoreRequestKindSections,
		Selector:   selector,
		Predicates: predicates,
		Shard:      shard,
		From:       from,
		Through:    through,
	}
	resp, ok := c.responses[newMetastoreRequestKey(req)]
	if !ok {
		return nil, fmt.Errorf("metastore responses missing for selector %q", expressionSignature(selector))
	}

	switch resp.Kind {
	case MetastoreRequestKindSections:
		filtered, err := filterDescriptorsForShard(req.Shard, resp.Sections)
		if err != nil {
			return nil, fmt.Errorf("filter descriptions for shard: %w", err)
		}
		return cloneFilteredShardDescriptors(filtered), nil
	default:
		// TODO(ivkalita): validate when storing the results, panic (?) here
		return nil, fmt.Errorf("unsupported metastore result kind %d", resp.Kind)
	}
}

func cloneExpression(expr Expression) Expression {
	if expr == nil {
		return nil
	}
	return expr.Clone()
}

func cloneExpressionSlice(exprs []Expression) []Expression {
	if len(exprs) == 0 {
		return nil
	}
	out := make([]Expression, len(exprs))
	for i, expr := range exprs {
		out[i] = cloneExpression(expr)
	}
	return out
}

func cloneFilteredShardDescriptors(descs []FilteredShardDescriptor) []FilteredShardDescriptor {
	if len(descs) == 0 {
		return nil
	}
	cloned := make([]FilteredShardDescriptor, len(descs))
	for i, desc := range descs {
		cloned[i] = FilteredShardDescriptor{
			Location:  desc.Location,
			Streams:   append([]int64(nil), desc.Streams...),
			Sections:  append([]int(nil), desc.Sections...),
			TimeRange: desc.TimeRange,
		}
	}
	return cloned
}

func PlanForMetastoreRequest(ctx context.Context, req MetastoreRequest, metastore metastore.Metastore) (*Plan, error) {
	p := &Plan{
		graph: dag.Graph[Node]{},
	}

	indexPaths, err := metastore.GetIndexes(ctx, req.From, req.Through)
	if err != nil {
		return nil, fmt.Errorf("metastore plan failed: %w", err)
	}

	parallelize := &Parallelize{
		NodeID: ulid.Make(),
	}
	p.graph.Add(parallelize)

	//TODO(ivkalita): ????????!!!??!
	noop := &Parallelize{
		NodeID: ulid.Make(),
	}
	p.graph.Add(noop)

	//noop := &TopK{
	//	NodeID: ulid.Make(),
	//	SortBy: &ColumnExpr{Ref: types.ColumnRef{Column: "min_timestamp.timestamp", Type: types.ColumnTypeBuiltin}},
	//	K:      math.MaxInt,
	//}
	//p.graph.Add(noop)

	scanSet := &ScanSet{
		NodeID: ulid.Make(),
	}
	p.graph.Add(scanSet)

	for _, indexPath := range indexPaths {
		scanSet.Targets = append(scanSet.Targets, &ScanTarget{
			Type: ScanTypePointers,
			PointersScan: &PointersScan{
				NodeID: ulid.Make(),
				// TODO(ivkalita): proper prefix config
				Location: DataObjLocation("index/v0/" + indexPath),

				Selector: req.Selector,

				Start: req.From,
				End:   req.Through,

				MaxTimeRange: TimeRange{
					Start: req.From,
					End:   req.Through,
				},
			},
		})
	}

	if err := p.graph.AddEdge(dag.Edge[Node]{Parent: parallelize, Child: noop}); err != nil {
		return nil, err
	}

	if err := p.graph.AddEdge(dag.Edge[Node]{Parent: noop, Child: scanSet}); err != nil {
		return nil, err
	}

	return p, nil
}
