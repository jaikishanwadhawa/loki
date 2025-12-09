package executor

import (
	"time"

	"github.com/grafana/loki/v3/pkg/dataobj/sections/streams"
	"github.com/grafana/loki/v3/pkg/engine/internal/planner/physical"
	"github.com/prometheus/prometheus/model/labels"
)

func buildStreamsPredicate(expr physical.Expression, start, end time.Time) (streams.RowPredicate, error) {
	matchers, err := physical.ExpressionToMatchers(expr, false)
	if err != nil {
		return nil, err
	}

	if len(matchers) == 0 {
		return nil, nil
	}

	return streamPredicateFromMatchers(start, end, matchers...), nil
}

func streamPredicateFromMatchers(start, end time.Time, matchers ...*labels.Matcher) streams.RowPredicate {
	if len(matchers) == 0 {
		return nil
	}

	predicates := make([]streams.RowPredicate, 0, len(matchers)+1)
	predicates = append(predicates, streams.TimeRangeRowPredicate{
		StartTime:    start,
		EndTime:      end,
		IncludeStart: true,
		IncludeEnd:   true,
	})
	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:
			predicates = append(predicates, streams.LabelMatcherRowPredicate{
				Name:  matcher.Name,
				Value: matcher.Value,
			})
		case labels.MatchNotEqual:
			predicates = append(predicates, streams.NotRowPredicate{
				Inner: streams.LabelMatcherRowPredicate{
					Name:  matcher.Name,
					Value: matcher.Value,
				},
			})
		case labels.MatchRegexp:
			predicates = append(predicates, streams.LabelFilterRowPredicate{
				Name: matcher.Name,
				Keep: func(_, value string) bool {
					return matcher.Matches(value)
				},
			})
		case labels.MatchNotRegexp:
			predicates = append(predicates, streams.NotRowPredicate{
				Inner: streams.LabelFilterRowPredicate{
					Name: matcher.Name,
					Keep: func(_, value string) bool {
						return !matcher.Matches(value)
					},
				},
			})
		}
	}

	if len(predicates) == 1 {
		return predicates[0]
	}

	current := predicates[0]

	for _, predicate := range predicates[1:] {
		and := streams.AndRowPredicate{
			Left:  predicate,
			Right: current,
		}
		current = and
	}
	return current
}
