package redisgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/995933447/log-go"
	"github.com/go-redis/redis/v8"
)

type Node struct {
	client *redis.Client
	host   string
	port   int
	hash   uint32
}

var notFoundNodeErr = errors.New("not found available redis node")

func hashKey(s string) uint32 {
	f := fnv.New32a()
	_, _ = f.Write([]byte(s))
	return f.Sum32()
}

func hashAddress(host string, port int) uint32 {
	return hashKey(fmt.Sprintf("%s:%d", host, port))
}

func NewNodeV2(host string, port int, password string, DB int) *Node {
	n := new(Node)
	n.host = host
	n.port = port
	n.hash = hashAddress(host, port)
	n.client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,
		DB:       DB,
	})
	return n
}

func NewNode(host string, port int, password string) *Node {
	return NewNodeV2(host, port, password, 0)
}

type nodeSorter struct {
	nodes []*Node
}

func (s *nodeSorter) Len() int {
	return len(s.nodes)
}

func (s *nodeSorter) Less(i, j int) bool {
	return s.nodes[i].hash < s.nodes[j].hash
}

func (s *nodeSorter) Swap(i, j int) {
	tmp := s.nodes[i]
	s.nodes[i] = s.nodes[j]
	s.nodes[j] = tmp
}

type Group struct {
	nodes  []*Node
	mu     sync.RWMutex
	logger *log.Logger
}

func NewGroup(nodes []*Node, logger *log.Logger) *Group {
	group := &Group{
		nodes:  nodes,
		logger: logger,
	}

	group.sortNodes()

	return group
}

func (g *Group) sortNodes() {
	i := &nodeSorter{nodes: g.nodes}
	sort.Sort(i)
	g.nodes = i.nodes
}

func (g *Group) FindNodeForAddress(host string, port int) *Node {
	return g.findNodeByHash(hashAddress(host, port))
}

func (g *Group) AddNode(node *Node) {
	g.nodes = append(g.nodes, node)
	g.sortNodes()
}

func (g *Group) findNodeByHash(hash uint32) *Node {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.nodes) == 0 {
		return nil
	}
	i := 0
	for i+1 < len(g.nodes) {
		if hash >= g.nodes[i].hash && hash < g.nodes[i+1].hash {
			return g.nodes[i]
		}
		i++
	}
	return g.nodes[len(g.nodes)-1]
}

func (g *Group) FindNodeForKey(key string) *Node {
	return g.findNodeByHash(hashKey(key))
}

func (g *Group) GetNodes() []*Node {
	return g.nodes
}

func (g *Group) GetClient(node *Node) *redis.Client {
	return node.client
}

func (g *Group) Set(ctx context.Context, key string, val []byte, exp time.Duration) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Set: key %s exp %v", key, exp)
	err := node.client.Set(ctx, key, val, exp).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}

	return nil
}

func (g *Group) SetUint64(ctx context.Context, key string, val uint64, exp time.Duration) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Set: key %s exp %v", key, exp)
	err := node.client.Set(ctx, key, val, exp).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}

	return nil
}

func (g *Group) SetRange(ctx context.Context, key string, offset int64, value string) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SetRange: key %s offset", key, offset)
	err := node.client.SetRange(ctx, key, offset, value).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	return nil
}

func (g *Group) SetByJson(ctx context.Context, key string, j interface{}, exp time.Duration) error {
	val, err := json.Marshal(j)
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	// 空串这里先不考虑
	if len(val) == 0 {
		return errors.New("unsupported empty value")
	}
	return g.Set(ctx, key, val, exp)
}

func (g *Group) HLen(ctx context.Context, key string) (uint32, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}

	g.logger.Infof(ctx, "redis: HLen: key %s", key)
	v := node.client.HLen(ctx, key)
	err := v.Err()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}

	return uint32(v.Val()), nil
}

func (g *Group) HSetByJson(ctx context.Context, key, subKey string, j interface{}, exp time.Duration) error {
	val, err := json.Marshal(j)
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HSetJson: key %s subKey %+v", key, subKey)
	err = node.client.HSet(ctx, key, subKey, string(val)).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}
	if exp > 0 {
		err = node.client.Expire(ctx, key, exp).Err()
		if err != nil {
			g.logger.Errorf(ctx, "err:%v", err)
			return err
		}
	}
	return nil
}

func (g *Group) HSetNXByJson(ctx context.Context, key, subKey string, j interface{}, exp time.Duration, setSuccess *bool) error {
	val, err := json.Marshal(j)
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HSetJsonNX: key %s subKey %+v exp %v", key, subKey, exp)
	res := node.client.HSetNX(ctx, key, subKey, string(val))
	err = res.Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}
	if exp > 0 {
		err = node.client.Expire(ctx, key, exp).Err()
		if err != nil {
			g.logger.Errorf(ctx, "err:%v", err)
			return err
		}
	}
	if setSuccess != nil {
		*setSuccess = res.Val()
	}
	return nil
}

func (g *Group) Get(ctx context.Context, key string) ([]byte, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}

	g.logger.Infof(ctx, "redis: Get: key %s", key)
	val, err := node.client.Get(ctx, key).Bytes()
	if err != nil {
		if err != redis.Nil {
			g.logger.Errorf(ctx, "err:%s", err)
		}
		return nil, err
	}
	return val, nil
}

func (g *Group) GetJson(ctx context.Context, key string, j interface{}) error {
	val, err := g.Get(ctx, key)
	if err != nil {
		if err == redis.Nil {
			return redis.Nil
		}
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	err = json.Unmarshal(val, j)
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	return nil
}

func (g *Group) GetUint64(ctx context.Context, key string) (uint64, error) {
	val, err := g.Get(ctx, key)
	if err != nil {
		if err == redis.Nil {
			return 0, redis.Nil
		}
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}

	i, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}

	return uint64(i), nil
}

func (g *Group) GetInt64(ctx context.Context, key string) (int64, error) {
	val, err := g.Get(ctx, key)
	if err != nil {
		if err == redis.Nil {
			return 0, redis.Nil
		}
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}

	i, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}

	return i, nil
}

func (g *Group) GetInt64Default(ctx context.Context, key string, def int64) (int64, error) {
	val, err := g.Get(ctx, key)
	if err != nil {
		if err == redis.Nil {
			return def, nil
		}
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}

	i, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}

	return i, nil
}

func (g *Group) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	g.logger.Infof(ctx, "redis: HGetAll: key %s", key)
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	return node.client.HGetAll(ctx, key).Result()
}

func (g *Group) HKeys(ctx context.Context, key string) ([]string, error) {
	g.logger.Infof(ctx, "redis: HKeys: key %s", key)
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	return node.client.HKeys(ctx, key).Result()
}

func (g *Group) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	g.logger.Infof(ctx, "redis: HKeys: key %s", key)
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, 0, notFoundNodeErr
	}
	return node.client.HScan(ctx, key, cursor, match, count).Result()
}

func (g *Group) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZKeys: key %s", key)
	return node.client.ZScan(ctx, key, cursor, match, count).Result()
}

func (g *Group) HMGetJson(ctx context.Context, key, subKey string, j interface{}) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HMGetJson: key %s subKey %+v", key, subKey)
	values, err := node.client.HMGet(ctx, key, subKey).Result()
	if err != nil {
		g.logger.Errorf(ctx, "redis HMGet err:%v", err)
		return err
	}
	if len(values) == 1 {
		v := values[0]
		if v != nil {
			var buf []byte
			if p, ok := v.(string); ok {
				buf = []byte(p)
			} else if p, ok := v.([]byte); ok {
				buf = p
			}
			if buf != nil {
				if len(buf) > 0 {
					err = json.Unmarshal(buf, j)
					if err != nil {
						g.logger.Errorf(ctx, "err:%s", err)
						return err
					}
				}
				return nil
			}
		}
	}
	return redis.Nil
}

func (g *Group) HDel(ctx context.Context, key string, subKey ...string) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HDel: key %s subKey %+v", key, subKey)
	delNum, err := node.client.HDel(ctx, key, subKey...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) Del(ctx context.Context, key string) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Del: key %s", key)
	err := node.client.Del(ctx, key).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	return nil
}

func (g *Group) ZAdd(ctx context.Context, key string, values ...*redis.Z) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZAdd: key %s", key)
	err := node.client.ZAdd(ctx, key, values...).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}
	return nil
}

func (g *Group) ZCount(ctx context.Context, key, min, max string) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZCount: key %s min %s - max %s", key, min, max)
	v := node.client.ZCount(ctx, key, min, max)
	err := v.Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return v.Val(), nil
}

func (g *Group) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZRangeByScore: key %s opt %v", key, opt)
	members, err := node.client.ZRangeByScore(ctx, key, opt).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) ([]redis.Z, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZRangeByScoreWithScores: key %s opt %v", key, opt)
	resultList, err := node.client.ZRangeByScoreWithScores(ctx, key, opt).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return resultList, nil
}

func (g *Group) ZIncrBy(ctx context.Context, key string, increment float64, member string) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZIncrBy: key %s increment %v member %s", key, increment, member)
	_, err := node.client.ZIncrBy(ctx, key, increment, member).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}
	return nil
}

func (g *Group) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZRange: key %s start %v stop %v", key, start, stop)
	members, err := node.client.ZRange(ctx, key, start, stop).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZRangeWithScores: key %s start %v stop %v", key, start, stop)
	resultList, err := node.client.ZRangeWithScores(ctx, key, start, stop).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return resultList, nil
}

func (g *Group) ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZRem: key %s", key)
	delNum, err := node.client.ZRem(ctx, key, members...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) ZRemRangeByScore(ctx context.Context, key string, min, max string) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZRemRangeByScore: key %s, min: %s, max: %s", key, min, max)
	delNum, err := node.client.ZRemRangeByScore(ctx, key, min, max).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) ZCard(ctx context.Context, key string) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZCard: key %s", key)
	num, err := node.client.ZCard(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return num, nil
}

func (g *Group) SAdd(ctx context.Context, key string, values ...interface{}) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SAdd: key %s", key)
	err := node.client.SAdd(ctx, key, values...).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}
	return nil
}

func (g *Group) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SRem: key %s", key)
	delNum, err := node.client.SRem(ctx, key, members...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return delNum, nil
}

func (g *Group) SCard(ctx context.Context, key string) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SCard: key %s", key)
	num, err := node.client.SCard(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return num, nil
}

func (g *Group) SIsMember(ctx context.Context, key string, members interface{}) (bool, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return false, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SIsMember: key %s", key)
	ok, err := node.client.SIsMember(ctx, key, members).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return false, err
	}
	return ok, nil
}

func (g *Group) SMembers(ctx context.Context, key string) ([]string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return nil, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SMembers: key %s", key)
	members, err := node.client.SMembers(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return members, nil
}

func (g *Group) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HIncrBy: key %s field %s incr %d", key, field, incr)
	n, err := node.client.HIncrBy(ctx, key, field, incr).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) IncrBy(ctx context.Context, key string, incr int64) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: IncrBy: key %s incr %d", key, incr)
	n, err := node.client.IncrBy(ctx, key, incr).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) DecrBy(ctx context.Context, key string, decr int64) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: DecrBy: key %s decr %d", key, decr)
	n, err := node.client.DecrBy(ctx, key, decr).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return 0, err
	}
	return n, nil
}

func (g *Group) HSet(ctx context.Context, key, field string, val interface{}) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HSet: key %s field %s", key, field)
	err := node.client.HSet(ctx, key, field, val).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}

	return nil
}

func (g *Group) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HMSet: key %s fields %v", key, fields)
	err := node.client.HMSet(ctx, key, fields).Err()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}

	return nil
}

func (g *Group) HGet(ctx context.Context, key, subKey string) (string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return "", notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HGet: key %s subKey %+v", key, subKey)
	val, err := node.client.HGet(ctx, key, subKey).Result()
	if err != nil {
		if err != redis.Nil {
			g.logger.Errorf(ctx, "err:%v", err)
		}
		return "", err
	}
	return val, nil
}

func (g *Group) HGetJson(ctx context.Context, key, subKey string, j interface{}) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HGetJson: key %s subKey %+v", key, subKey)
	val, err := node.client.HGet(ctx, key, subKey).Result()
	if err != nil {
		if err != redis.Nil {
			g.logger.Errorf(ctx, "err:%v", err)
		}
		return err
	}
	err = json.Unmarshal([]byte(val), j)
	if err != nil {
		g.logger.Errorf(ctx, "err:%s", err)
		return err
	}
	return nil
}

func (g *Group) Expire(ctx context.Context, key string, expiration time.Duration) error {
	node := g.FindNodeForKey(key)
	if node == nil {
		return notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Expire: key %s exp %+v", key, expiration)
	_, err := node.client.Expire(ctx, key, expiration).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return err
	}
	return nil
}

func (g *Group) Exists(ctx context.Context, key string) (bool, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return false, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Exists: key %s", key)
	val, err := node.client.Exists(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return false, err
	}
	if val == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (g *Group) HExists(ctx context.Context, key, field string) (bool, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return false, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: HExists: key %s field %s", key, field)
	exists, err := node.client.HExists(ctx, key, field).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return false, err
	}
	return exists, nil
}

func (g *Group) ScriptRun(ctx context.Context, lua string, keys []string, args ...interface{}) (interface{}, error) {
	node := g.FindNodeForKey(keys[0])
	if node == nil {
		return nil, notFoundNodeErr
	}
	script := redis.NewScript(lua)
	g.logger.Infof(ctx, "lua run:%s", lua)
	result, err := script.Run(ctx, node.client, keys, args...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return result, nil
}

func (g *Group) EvalSha(ctx context.Context, luaSha1 string, keys []string, args ...interface{}) (interface{}, error) {
	node := g.FindNodeForKey(keys[0])
	if node == nil {
		return nil, notFoundNodeErr
	}
	g.logger.Infof(ctx, "lua eval:%s", luaSha1)
	result, err := node.client.EvalSha(ctx, luaSha1, keys, args...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return nil, err
	}
	return result, nil
}

func (g *Group) ScriptLoad(ctx context.Context, luaScript string) (string, error) {
	node := g.FindNodeForKey("")
	if node == nil {
		return "", notFoundNodeErr
	}
	g.logger.Infof(ctx, "lua load:%s", luaScript)
	luaSha1, err := node.client.ScriptLoad(ctx, luaScript).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return "", err
	}
	return luaSha1, nil
}

func (g *Group) Incr(ctx context.Context, key string) (int64, error) {
	node := g.FindNodeForKey("")
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Incr: key %s", key)
	val, err := node.client.Incr(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return val, nil
}

func (g *Group) Decr(ctx context.Context, key string) (int64, error) {
	node := g.FindNodeForKey("")
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Decr: key %s", key)
	val, err := node.client.Decr(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return val, nil
}

func (g *Group) ExpireAt(ctx context.Context, key string, expiredAt time.Time) (bool, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return false, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ExpireAt: key %s exp %v", key, expiredAt)
	ok, err := node.client.ExpireAt(ctx, key, expiredAt).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return false, err
	}
	return ok, nil
}

func (g *Group) LPop(ctx context.Context, key string) (string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return "", notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: LPop: key %s", key)
	val, err := node.client.LPop(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return "", err
	}
	return val, nil
}

func (g *Group) RPop(ctx context.Context, key string) (string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return "", notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: RPop: key %s", key)
	val, err := node.client.RPop(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return "", err
	}
	return val, nil
}

func (g *Group) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: LPush: key %s", key)
	count, err := node.client.LPush(ctx, key, values...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return count, nil
}

func (g *Group) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: RPush: key %s", key)
	count, err := node.client.RPush(ctx, key, values...).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return count, nil
}

func (g *Group) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return []string{}, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: LRange: key %s start %d stop %d", key, start, stop)
	result, err := node.client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return []string{}, err
	}
	return result, nil
}

func (g *Group) LTrim(ctx context.Context, key string, start, stop int64) (string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return "", notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: LTrim: key %s start %d stop %d", key, start, stop)
	result, err := node.client.LTrim(ctx, key, start, stop).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return "", err
	}
	return result, nil
}

func (g *Group) LLen(ctx context.Context, key string) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: LLen: key %s", key)
	count, err := node.client.LLen(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return count, nil
}

func (g *Group) LIndex(ctx context.Context, key string, index int64) (string, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return "", notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: LIndex: key %s index %d", key, index)
	val, err := node.client.LIndex(ctx, key, index).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return "", err
	}
	return val, nil
}

func (g *Group) SetNX(ctx context.Context, key string, val []byte, exp time.Duration) (bool, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return false, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: SetNX: key %s exp %v", key, exp)
	b, err := node.client.SetNX(ctx, key, val, exp).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return false, err
	}
	return b, nil
}

func (g *Group) ZScore(ctx context.Context, key string, member string) (float64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: ZScore: key %s member %s", key, member)
	score, err := node.client.ZScore(ctx, key, member).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return score, nil
}

func (g *Group) Ttl(ctx context.Context, key string) (time.Duration, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: TTL: key %s", key)
	ttl, err := node.client.TTL(ctx, key).Result()
	if err != nil {
		g.logger.Errorf(ctx, "err:%v", err)
		return 0, err
	}
	return ttl, nil
}

func (g *Group) SetBit(ctx context.Context, key string, offset int64, val int) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Set Bit: key %s offset %d val %d", key, offset, val)
	intCmd := node.client.SetBit(ctx, key, offset, val)
	if intCmd.Err() != nil {
		g.logger.Errorf(ctx, "err:%s", intCmd.Err())
		return 0, intCmd.Err()
	}

	return intCmd.Result()
}

func (g *Group) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: Set Bit: key %s offset %d", key, offset)
	intCmd := node.client.GetBit(ctx, key, offset)
	if intCmd.Err() != nil {
		g.logger.Errorf(ctx, "err:%s", intCmd.Err())
		return 0, intCmd.Err()
	}
	return intCmd.Result()
}

func (g *Group) BitCount(ctx context.Context, key string, bitCount *redis.BitCount) (int64, error) {
	node := g.FindNodeForKey(key)
	if node == nil {
		return 0, notFoundNodeErr
	}
	g.logger.Infof(ctx, "redis: BitCount: key %s", key)
	intCmd := node.client.BitCount(ctx, key, bitCount)
	if intCmd.Err() != nil {
		g.logger.Errorf(ctx, "err:%s", intCmd.Err())
		return 0, intCmd.Err()
	}
	return intCmd.Result()
}

func (g *Group) FlushAll(ctx context.Context) (string, error) {
	g.logger.Infof(ctx, "redis: FushAll")
	var intCmd *redis.StatusCmd
	for _, node := range g.nodes {
		intCmd = node.client.FlushAll(ctx)
		if intCmd.Err() != nil {
			g.logger.Errorf(ctx, "err:%s", intCmd.Err())
			return "", intCmd.Err()
		}
	}

	return intCmd.Result()
}
