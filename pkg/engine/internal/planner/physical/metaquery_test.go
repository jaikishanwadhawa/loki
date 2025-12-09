package physical

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj/metastore"
	"github.com/grafana/loki/v3/pkg/engine/internal/types"
)

func TestMetaqueryCollectorRequests(t *testing.T) {
	t.Parallel()

	collector := NewMetastoreCollectorCatalog()
	selector := &ColumnExpr{Ref: types.ColumnRef{Column: "__name__", Type: types.ColumnTypeLabel}}
	from := time.Unix(0, 0)
	through := from.Add(time.Minute)
	shard := ShardInfo{Shard: 0, Of: 1}

	_, err := collector.ResolveShardDescriptorsWithShard(selector, nil, shard, from, through)
	require.NoError(t, err)

	// Duplicate request should be ignored.
	_, err = collector.ResolveShardDescriptorsWithShard(selector.Clone(), nil, shard, from, through)
	require.NoError(t, err)

	requests := collector.Requests()
	require.Len(t, requests, 1)
	require.Equal(t, MetastoreRequestKindSections, requests[0].Kind)
	require.Equal(t, selector.String(), requests[0].Selector.String())
	require.Equal(t, shard, requests[0].Shard)
	require.Equal(t, from, requests[0].From)
	require.Equal(t, through, requests[0].Through)
}

func TestMetaqueryPreparedCatalogResolve(t *testing.T) {
	t.Parallel()

	prepared := NewMetaqueryPreparedCatalog()
	selector := &ColumnExpr{Ref: types.ColumnRef{Column: "foo", Type: types.ColumnTypeLabel}}
	shard := ShardInfo{Shard: 0, Of: 1}
	from := time.Unix(0, 0)
	through := from.Add(time.Minute)

	req := MetastoreRequest{
		Kind:     MetastoreRequestKindSections,
		Selector: selector,
		Shard:    shard,
		From:     from,
		Through:  through,
	}

	resp := MetastoreResponse{
		Kind: MetastoreRequestKindSections,

		Sections: []*metastore.DataobjSectionDescriptor{
			{
				SectionKey: metastore.SectionKey{
					ObjectPath: "obj",
					SectionIdx: 2,
				},
				StreamIDs: []int64{1},
			},
		},
	}

	desc := FilteredShardDescriptor{
		Location: "obj",
		Streams:  []int64{1},
		Sections: []int{2},
	}
	require.NoError(t, prepared.Store(req, resp))

	got, err := prepared.ResolveShardDescriptorsWithShard(selector.Clone(), nil, shard, from, through)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, desc.Location, got[0].Location)
	require.Equal(t, desc.Streams, got[0].Streams)

	// Mutating the returned descriptor must not affect future calls.
	got[0].Streams[0] = 42
	gotAgain, err := prepared.ResolveShardDescriptorsWithShard(selector, nil, shard, from, through)
	require.NoError(t, err)
	require.Equal(t, int64(1), gotAgain[0].Streams[0])
}
