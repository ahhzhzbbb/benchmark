package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	astypes "github.com/aerospike/aerospike-client-go/v7/types"
)

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

	// ClientPolicy PHẢI được cấu hình trước khi tạo client; set sau sẽ không có tác dụng.
	clientPolicy := as.NewClientPolicy()
	clientPolicy.Timeout = 5 * time.Second // giống DialTimeout của Redis adapter
	// Tắt circuit breaker phía client (MAX_ERROR_RATE): benchmark cần thấy lỗi thật của server.
	clientPolicy.MaxErrorRate = 0
	// Tăng ConnectionQueueSize và cho phép mở thêm kết nối nếu cần để không bao giờ bị nghẽn do cạn pool
	clientPolicy.ConnectionQueueSize = 4096
	clientPolicy.LimitConnectionsToQueueSize = false
	clientPolicy.OpeningConnectionThreshold = 0
	clientPolicy.IdleTimeout = 55 * time.Second
	client, err := as.NewClientWithPolicyAndHost(clientPolicy, hosts...)
	if err != nil {
		return nil, err
	}

	// MaxRetries=0: không retry để latency/lỗi phản ánh đúng 1 lần gọi (giống Redis adapter).
	readPolicy := as.NewPolicy()
	readPolicy.TotalTimeout = timeout
	readPolicy.SocketTimeout = timeout
	readPolicy.MaxRetries = 0

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.TotalTimeout = timeout
	writePolicy.SocketTimeout = timeout
	writePolicy.MaxRetries = 0

	// Query policy tương đương FT.SEARCH ... TIMEOUT 2000 NOCONTENT LIMIT 0 10:
	//   - IncludeBinData=false: chỉ trả về key/digest, giống NOCONTENT của Redis
	//   - MaxRecords=queryResultLimit: server dừng ngay sau khi lấy đủ số bản ghi (gửi _INFO3_LAST)
	//   - ExpectedDuration=SHORT / ShortQuery=true: báo cho Aerospike Server chạy query inline trực tiếp
	//     trên service thread (giống GET), bỏ qua query monitor và query thread pool overhead
	//   - RecordQueueSize: buffer cho channel kết quả để worker không bị nghẽn khi nhận stream
	queryTimeout := time.Duration(queryTimeoutMs) * time.Millisecond
	queryPolicy := as.NewQueryPolicy()
	queryPolicy.TotalTimeout = queryTimeout
	queryPolicy.SocketTimeout = queryTimeout
	queryPolicy.IncludeBinData = false
	queryPolicy.MaxRecords = queryResultLimit
	queryPolicy.ExpectedDuration = as.SHORT
	queryPolicy.ShortQuery = true
	queryPolicy.MaxRetries = 0
	queryPolicy.RecordQueueSize = 512

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

// Query thực thi truy vấn Secondary Index trên Aerospike dựa trên cấu trúc QuerySpec.
// Hoạt động độc lập, gọi trực tiếp API native của Aerospike SDK mà không cần parse chuỗi hay regex.
func (a *AerospikeDB) Query(ctx context.Context, query *QuerySpec) error {
	if query == nil {
		return errors.New("nil query spec")
	}

	timeout, err := ctxTimeout(ctx, a.queryPolicy.TotalTimeout)
	if err != nil {
		return err
	}

	stmt := as.NewStatement(a.namespace, a.setName)
	if !query.IsRange {
		stmt.SetFilter(as.NewEqualFilter(query.Bin, query.Value))
	} else {
		stmt.SetFilter(as.NewRangeFilter(query.Bin, query.Min, query.Max))
	}

	// Copy policy dùng chung để gắn timeout riêng cho query này (an toàn đa luồng).
	policy := *a.queryPolicy
	policy.TotalTimeout, policy.SocketTimeout = timeout, timeout

	rs, qerr := a.client.Query(&policy, stmt)
	if qerr != nil {
		return qerr
	}

	// Đọc hết toàn bộ channel kết quả mà server gửi để socket kết thúc tự nhiên (_INFO3_LAST),
	// giúp kết nối TCP được trả về connection pool một cách nguyên vẹn (KeepConnection = true).
	// KHÔNG gọi rs.Close() giữa chừng khi đạt limit, vì việc hủy ngang luồng đọc sẽ làm
	// SDK trả về lỗi QUERY_TERMINATED và ĐÓNG/TIÊU HỦY toàn bộ socket TCP tới các node (KeepConnection = false),
	// gây ra hiện tượng bão mở/đóng kết nối (connection storm) làm sụp đổ TPS trong open-loop mode.
	// rs.Close() chỉ được gọi khi ctx bị timeout hoặc bị hủy.
	var queryErr error
	results := rs.Results()
	for {
		select {
		case <-ctx.Done():
			rs.Close()
			return ctx.Err()
		case res, ok := <-results:
			if !ok {
				return queryErr
			}
			if res.Err != nil && queryErr == nil {
				queryErr = res.Err
			}
		}
	}
}
