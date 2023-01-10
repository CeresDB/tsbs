package ceresdb

import (
	"fmt"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/timescale/tsbs/pkg/data"
)

type batch struct {
	rowCount    uint64
	metricCount uint64
	rows        []*types.Row
}

func (b *batch) Len() uint {
	return uint(b.rowCount)
}

func unixTimestampMs(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func valueToFloat64(v lineprotocol.Value) float64 {
	switch v.Kind() {
	case lineprotocol.Int:
		return float64(v.IntV())
	case lineprotocol.Float:
		return v.FloatV()
	default:
		panic(fmt.Sprintf("not support field type: %v", v))
	}
}
func (b *batch) Append(item data.LoadedPoint) {
	data := item.Data.([]byte)
	dec := lineprotocol.NewDecoderWithBytes(data)

	for dec.Next() {
		b.rowCount++
		m, err := dec.Measurement()
		if err != nil {
			panic(err)
		}

		builder := ceresdb.NewRowBuilder(string(m))

		for {
			key, val, err := dec.NextTag()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			builder.AddTag(string(key), string(val))
		}

		for {
			key, val, err := dec.NextField()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			builder.AddField(string(key), valueToFloat64(val))
		}

		t, err := dec.Time(lineprotocol.Nanosecond, time.Time{})
		if err != nil {
			panic(err)
		}
		builder.SetTimestamp(unixTimestampMs(t))

		row, err := builder.Build()
		if err != nil {
			panic(err)
		}

		b.metricCount += uint64(len(row.Fields))

		b.rows = append(b.rows, row)
	}
}
