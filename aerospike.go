package main

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

// tagRegex: @field:{value} (loại trừ value chứa '*' — đó là prefix query).
var tagRegex = regexp.MustCompile(`@(\w+):\{([^}*]+)\}`)
var numRegex = regexp.MustCompile(`@(\w+):\[(\d+)\s+(\d+)\]`)

// prefixRegex: @field:{prefix*} (cú pháp prefix trên TAG field của RediSearch).
var prefixRegex = regexp.MustCompile(`@(\w+):\{([^}*]+)\*\}`)

// defaultAerospikeTimeout khớp với ReadTimeout/WriteTimeout của Redis adapter.
const defaultAerospikeTimeout = 3 * time.Second

type AerospikeDB struct {
	client    *as.Client
	namespace string
	setName   string
	timeout   time.Duration

	// Policy được tạo một lần và dùng chung (client chỉ đọc policy).
	// Khi ctx có deadline ngắn hơn timeout, policy sẽ được copy và chỉnh riêng.
	readPolicy  *as.BasePolicy
	writePolicy *as.WritePolicy
	queryPolicy *as.QueryPolicy
}

// parseAerospikeHost parse "host" hoặc "host:port" (mặc định port 3000).
func parseAerospikeHost(s string) (*as.Host, error) {
	host, port := s, 3000
	if i := strings.LastIndex(s, ":"); i >= 0 {
		host = s[:i]
		p, err := strconv.Atoi(s[i+1:])
		if err != nil {
			return nil, fmt.Errorf("port không hợp lệ trong %q: %w", s, err)
		}
		port = p
	}
	if host == "" {
		return nil, fmt.Errorf("host rỗng trong %q", s)
	}
	return as.NewHost(host, port), nil
}

func NewAerospikeDB(cfg AerospikeCfg) (*AerospikeDB, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("không có host nào được cấu hình cho Aerospike")
	}

	// Truyền TẤT CẢ host làm seed: client vẫn kết nối được khi một node chết.
	hosts := make([]*as.Host, 0, len(cfg.Hosts))
	for _, h := range cfg.Hosts {
		host, err := parseAerospikeHost(h)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}

	timeout := cfg.Timeout.Duration
	if timeout <= 0 {
		timeout = defaultAerospikeTimeout
	}

	clientPolicy := as.NewClientPolicy()
	clientPolicy.Timeout = 5 * time.Second // giống DialTimeout của Redis adapter
	client, err := as.NewClientWithPolicyAndHost(clientPolicy, hosts...)
	if err != nil {
		return nil, err
	}

	readPolicy := as.NewPolicy()
	readPolicy.TotalTimeout = timeout
	readPolicy.SocketTimeout = timeout

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.TotalTimeout = timeout
	writePolicy.SocketTimeout = timeout

	// Query policy tương đương FT.SEARCH ... TIMEOUT 2000 NOCONTENT LIMIT 0 10:
	//   - IncludeBinData=false: chỉ trả về key/digest, giống NOCONTENT của Redis
	//   - MaxRecords: giới hạn phía server, giống LIMIT của Redis
	queryTimeout := time.Duration(queryTimeoutMs) * time.Millisecond
	queryPolicy := as.NewQueryPolicy()
	queryPolicy.TotalTimeout = queryTimeout
	queryPolicy.SocketTimeout = queryTimeout
	queryPolicy.IncludeBinData = false
	queryPolicy.MaxRecords = queryResultLimit

	return &AerospikeDB{
		client:      client,
		namespace:   cfg.Namespace,
		setName:     cfg.Set,
		timeout:     timeout,
		readPolicy:  readPolicy,
		writePolicy: writePolicy,
		queryPolicy: queryPolicy,
	}, nil
}

// ctxTimeout trả về timeout hiệu lực: min(def, thời gian còn lại của ctx).
// Trả về lỗi nếu ctx đã bị huỷ/hết hạn.
func ctxTimeout(ctx context.Context, def time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if dl, ok := ctx.Deadline(); ok {
		if remain := time.Until(dl); remain < def {
			if remain <= 0 {
				return 0, context.DeadlineExceeded
			}
			return remain, nil
		}
	}
	return def, nil
}

func (a *AerospikeDB) readPolicyFor(ctx context.Context) (*as.BasePolicy, error) {
	t, err := ctxTimeout(ctx, a.readPolicy.TotalTimeout)
	if err != nil || t == a.readPolicy.TotalTimeout {
		return a.readPolicy, err
	}
	p := *a.readPolicy
	p.TotalTimeout, p.SocketTimeout = t, t
	return &p, nil
}

func (a *AerospikeDB) writePolicyFor(ctx context.Context) (*as.WritePolicy, error) {
	t, err := ctxTimeout(ctx, a.writePolicy.TotalTimeout)
	if err != nil || t == a.writePolicy.TotalTimeout {
		return a.writePolicy, err
	}
	p := *a.writePolicy
	p.TotalTimeout, p.SocketTimeout = t, t
	return &p, nil
}

func (a *AerospikeDB) Name() string {
	return "Aerospike"
}

func (a *AerospikeDB) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
			select {
			case <-task.OnComplete():
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (a *AerospikeDB) Get(ctx context.Context, keyStr string) error {
	policy, err := a.readPolicyFor(ctx)
	if err != nil {
		return err
	}
	key, kerr := as.NewKey(a.namespace, a.setName, keyStr)
	if kerr != nil {
		return kerr
	}
	if _, gerr := a.client.Get(policy, key); gerr != nil {
		if gerr.Matches(astypes.KEY_NOT_FOUND_ERROR) {
			return ErrNotFound
		}
		return gerr
	}
	return nil
}

// Set ghi bins đã được dựng sẵn bởi generator (không cần xử lý trong vùng đo latency).
func (a *AerospikeDB) Set(ctx context.Context, keyStr string, payload *Payload) error {
	policy, err := a.writePolicyFor(ctx)
	if err != nil {
		return err
	}
	key, kerr := as.NewKey(a.namespace, a.setName, keyStr)
	if kerr != nil {
		return kerr
	}
	if perr := a.client.Put(policy, key, as.BinMap(payload.Bins)); perr != nil {
		return perr
	}
	return nil
}

func (a *AerospikeDB) Delete(ctx context.Context, keyStr string) error {
	policy, err := a.writePolicyFor(ctx)
	if err != nil {
		return err
	}
	key, kerr := as.NewKey(a.namespace, a.setName, keyStr)
	if kerr != nil {
		return kerr
	}
	existed, derr := a.client.Delete(policy, key)
	if derr != nil {
		return derr
	}
	if !existed {
		return ErrNotFound
	}
	return nil
}

// parseQueryExpr parse chuỗi query RediSearch (TAG, NUMERIC, OR, PREFIX) sang Aerospike Filter và FilterExpression.
//
// OR query: Aerospike chỉ dùng được tối đa 1 Secondary Index/query nên không thể
// phục vụ nhiều nhánh bằng SI → mọi điều kiện của mọi nhánh đều chuyển thành
// expression, gộp bằng ExpOr, và query chạy dạng set scan.
func parseQueryExpr(query string) (*as.Filter, *as.Expression) {
	if strings.Contains(query, "|") {
		var branchExps []*as.Expression
		for _, part := range strings.Split(query, "|") {
			if _, exp := parseSingleExpr(part, false); exp != nil {
				branchExps = append(branchExps, exp)
			}
		}
		switch len(branchExps) {
		case 0:
			return nil, nil
		case 1:
			return nil, branchExps[0]
		default:
			return nil, as.ExpOr(branchExps...)
		}
	}
	return parseSingleExpr(query, true)
}

// parseSingleExpr parse một nhánh AND. useSI=true: điều kiện TAG/NUMERIC đầu tiên
// dùng Secondary Index, phần còn lại là expression. useSI=false: tất cả là expression.
func parseSingleExpr(query string, useSI bool) (*as.Filter, *as.Expression) {
	tags := tagRegex.FindAllStringSubmatch(query, -1)
	nums := numRegex.FindAllStringSubmatch(query, -1)
	prefixes := prefixRegex.FindAllStringSubmatch(query, -1)

	var exps []*as.Expression
	var siFilter *as.Filter

	for _, match := range tags {
		bin := match[1]
		val := match[2]
		if useSI && siFilter == nil {
			siFilter = as.NewEqualFilter(bin, val)
		} else {
			exps = append(exps, as.ExpEq(as.ExpStringBin(bin), as.ExpStringVal(val)))
		}
	}
	for _, match := range nums {
		bin := match[1]
		min, _ := strconv.ParseInt(match[2], 10, 64)
		max, _ := strconv.ParseInt(match[3], 10, 64)
		if useSI && siFilter == nil {
			siFilter = as.NewRangeFilter(bin, min, max)
		} else {
			exps = append(exps, as.ExpGreaterEq(as.ExpIntBin(bin), as.ExpIntVal(min)))
			exps = append(exps, as.ExpLessEq(as.ExpIntBin(bin), as.ExpIntVal(max)))
		}
	}
	for _, match := range prefixes {
		bin := match[1]
		prefix := regexp.QuoteMeta(match[2])
		exps = append(exps, as.ExpRegexCompare("^"+prefix, 0, as.ExpStringBin(bin)))
	}

	var finalExp *as.Expression
	if len(exps) == 1 {
		finalExp = exps[0]
	} else if len(exps) > 1 {
		finalExp = as.ExpAnd(exps...)
	}

	return siFilter, finalExp
}

func (a *AerospikeDB) Query(ctx context.Context, query string) error {
	timeout, err := ctxTimeout(ctx, a.queryPolicy.TotalTimeout)
	if err != nil {
		return err
	}

	stmt := as.NewStatement(a.namespace, a.setName)
	siFilter, filterExp := parseQueryExpr(query)

	if siFilter != nil {
		stmt.SetFilter(siFilter)
	}

	// Copy policy dùng chung để gắn FilterExpression/timeout riêng cho query này.
	policy := *a.queryPolicy
	policy.TotalTimeout, policy.SocketTimeout = timeout, timeout
	policy.FilterExpression = filterExp

	rs, qerr := a.client.Query(&policy, stmt)
	if qerr != nil {
		return qerr
	}
	defer rs.Close()

	results := rs.Results()
	count := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-results:
			if !ok {
				return nil
			}
			if res.Err != nil {
				return res.Err
			}
			count++
			if count >= queryResultLimit {
				return nil
			}
		}
	}
}
