package ceresdb

import (
	"context"
	"log"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	addr   string
	client ceresdb.Client
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	client, err := ceresdb.NewClient(p.addr)
	if err != nil {
		panic(err)
	}
	p.client = client
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metricCount, batch.rowCount
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) do(b *batch) (uint64, uint64) {
	for {
		ret, err := p.client.Write(context.TODO(), b.rows)

		if err == nil {
			// log.Printf("success :%d\n", ret)
			return b.metricCount, b.rowCount
		}

		log.Printf("Retrying, write failed. err:%s, ret:%d", err, ret)
		time.Sleep(time.Millisecond * 10)
	}
}
