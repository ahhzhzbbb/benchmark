package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v7"
)

var tagRegex = regexp.MustCompile(`@(\w+):{([^}]+)}`)
var numRegex = regexp.MustCompile(`@(\w+):\[(\d+)\s+(\d+)\]`)

type AerospikeDB struct {
	client    *as.Client
	namespace string
	setName   string
}

func NewAerospikeDB(cfg AerospikeCfg) (*AerospikeDB, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("không có host nào được cấu hình cho Aerospike")
	}
	hostPort := strings.Split(cfg.Hosts[0], ":")
	host := hostPort[0]
	port := 3000
	if len(hostPort) > 1 {
		p, err := strconv.Atoi(hostPort[1])
		if err == nil {
			port = p
		}
	}

	clientPolicy := as.NewClientPolicy()
	client, err := as.NewClientWithPolicyAndHost(clientPolicy, as.NewHost(host, port))
	if err != nil {
		return nil, err
	}

	return &AerospikeDB{
		client:    client,
		namespace: cfg.Namespace,
		setName:   cfg.Set,
	}, nil
}

func (a *AerospikeDB) Name() string {
	return "Aerospike"
}

func (a *AerospikeDB) Ping(ctx context.Context) error {
	if !a.client.IsConnected() {
		return fmt.Errorf("aerospike client disconnected")
	}
	return nil
}

func (a *AerospikeDB) Close() error {
	a.client.Close()
	return nil
}

func (a *AerospikeDB) Init(ctx context.Context) error {
	indexes := map[string]as.IndexType{
		"Dnn":          as.STRING,
		"IPType":       as.STRING,
		"State":        as.STRING,
		"Timer":        as.NUMERIC,
		"SEID":         as.STRING,
		"startTime":    as.NUMERIC,
		"attachTime":   as.NUMERIC,
		"FTEIDC":       as.STRING,
		"IP":           as.STRING,
		"SNSSAI":       as.STRING,
		"UpfSelection": as.NUMERIC,
		"TEIDC":        as.STRING,
	}

	for binName, idxType := range indexes {
		idxName := "idx_" + binName
		task, err := a.client.CreateIndex(nil, a.namespace, a.setName, idxName, binName, idxType)
		if err != nil {
			if strings.Contains(err.Error(), "Index already exists") || strings.Contains(err.Error(), "already exists") {
				continue
			}
			return err
		}
		if task != nil {
			<-task.OnComplete()
		}
	}
	return nil
}

func (a *AerospikeDB) Get(ctx context.Context, keyStr string) error {
	key, err := as.NewKey(a.namespace, a.setName, keyStr)
	if err != nil {
		return err
	}
	policy := as.NewPolicy()
	_, err = a.client.Get(policy, key)
	if err != nil && strings.Contains(err.Error(), "Key not found") {
		return nil // Ignore not found for benchmark
	}
	return err
}

func (a *AerospikeDB) Set(ctx context.Context, keyStr string, value []byte) error {
	key, err := as.NewKey(a.namespace, a.setName, keyStr)
	if err != nil {
		return err
	}
	var record Record
	if err := json.Unmarshal(value, &record); err != nil {
		return err
	}

	bins := as.BinMap{
		"Dnn":          record.Dnn,
		"IPType":       record.IPType,
		"State":        record.State,
		"Timer":        record.Timer,
		"SEID":         record.SEID,
		"startTime":    record.StartTime,
		"attachTime":   record.AttachTime,
		"FTEIDC":       record.FTEIDC,
		"IP":           record.IP,
		"SNSSAI":       record.SNSSAI,
		"UpfSelection": record.UpfSelection,
		"payload":      record.Payload,
		"TEIDC":        record.TEIDC,
	}
	policy := as.NewWritePolicy(0, 0)
	return a.client.Put(policy, key, bins)
}

func (a *AerospikeDB) Delete(ctx context.Context, keyStr string) error {
	key, err := as.NewKey(a.namespace, a.setName, keyStr)
	if err != nil {
		return err
	}
	policy := as.NewWritePolicy(0, 0)
	_, err = a.client.Delete(policy, key)
	return err
}

func (a *AerospikeDB) Query(ctx context.Context, query string) error {
	stmt := as.NewStatement(a.namespace, a.setName)

	tags := tagRegex.FindAllStringSubmatch(query, -1)
	nums := numRegex.FindAllStringSubmatch(query, -1)

	var exps []*as.Expression
	var siFilter *as.Filter

	for _, match := range tags {
		bin := match[1]
		val := match[2]
		if siFilter == nil {
			siFilter = as.NewEqualFilter(bin, val)
		} else {
			exps = append(exps, as.ExpEq(as.ExpStringBin(bin), as.ExpStringVal(val)))
		}
	}
	for _, match := range nums {
		bin := match[1]
		min, _ := strconv.ParseInt(match[2], 10, 64)
		max, _ := strconv.ParseInt(match[3], 10, 64)
		if siFilter == nil {
			siFilter = as.NewRangeFilter(bin, min, max)
		} else {
			exps = append(exps, as.ExpGreaterEq(as.ExpIntBin(bin), as.ExpIntVal(min)))
			exps = append(exps, as.ExpLessEq(as.ExpIntBin(bin), as.ExpIntVal(max)))
		}
	}

	if siFilter != nil {
		stmt.SetFilter(siFilter)
	}

	var finalExp *as.Expression
	if len(exps) == 1 {
		finalExp = exps[0]
	} else if len(exps) > 1 {
		finalExp = as.ExpAnd(exps...)
	}

	policy := as.NewQueryPolicy()
	policy.FilterExpression = finalExp

	rs, err := a.client.Query(policy, stmt)
	if err != nil {
		return err
	}

	count := 0
	for res := range rs.Results() {
		if res.Err != nil {
			return res.Err
		}
		count++
		if count >= 10 {
			rs.Close()
			break
		}
	}
	return nil
}
