package data

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheTokenByTable(t *testing.T) {
	name := "test_conn"
	base := cacheToken(name, []string{"user"})
	cacheTouchTable(name, "order")
	afterOther := cacheToken(name, []string{"user"})
	if base != afterOther {
		t.Fatalf("user cache token should not change when order touched")
	}
	cacheTouchTable(name, "user")
	afterUser := cacheToken(name, []string{"user"})
	if base == afterUser {
		t.Fatalf("user cache token should change when user touched")
	}
}

func TestCacheInvalidateByTable(t *testing.T) {
	name := "test_conn_invalidate"
	key := "q:test:key"
	cacheMap(name).Store(key, cacheValue{expireAt: 0})
	cacheTrackKey(name, key, []string{"user"})

	if _, ok := cacheMap(name).Load(key); !ok {
		t.Fatalf("cache key missing before invalidation")
	}

	cacheTouchTable(name, "order")
	if _, ok := cacheMap(name).Load(key); !ok {
		t.Fatalf("cache key should not be invalidated by other table")
	}

	cacheTouchTable(name, "user")
	if _, ok := cacheMap(name).Load(key); ok {
		t.Fatalf("cache key should be invalidated by same table")
	}
}

func TestCacheInvalidateTopicUsesMessage(t *testing.T) {
	name := "test_conn_message"
	key := "q:test:message"
	cacheMap(name).Store(key, cacheValue{expireAt: 0})
	cacheTrackKey(name, key, []string{"user"})

	if _, ok := cacheMap(name).Load(key); !ok {
		t.Fatalf("cache key missing before message invalidate")
	}

	if _, _, found := host.InvokeLocalService(nil, cacheInvalidateTopic, map[string]interface{}{
		"base":  name,
		"table": "user",
	}); found {
		t.Fatalf("cache invalidate topic should not be registered as service")
	}

	if _, _, found := host.InvokeLocalMessage(nil, cacheInvalidateTopic, map[string]interface{}{
		"base":  name,
		"table": "user",
	}); !found {
		t.Fatalf("cache invalidate topic should be registered as message")
	}

	if _, ok := cacheMap(name).Load(key); ok {
		t.Fatalf("cache key should be invalidated by message handler")
	}
}

func TestCacheSingleflightCoalescesConcurrentLoads(t *testing.T) {
	name := "singleflight"
	key := "q:test"
	var runs atomic.Int32
	start := make(chan struct{})
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	results := make([]int64, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			val, _, err := cacheSingleflight(name, key, func() (cacheValue, error) {
				runs.Add(1)
				once.Do(func() { close(entered) })
				<-release
				return cacheValue{total: 42}, nil
			})
			results[i] = val.total
			errs[i] = err
		}(i)
	}
	close(start)
	<-entered
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()
	if runs.Load() != 1 {
		t.Fatalf("expected one loader execution, got %d", runs.Load())
	}
	stats := statsFor(name)
	if stats.CacheFlight.Load() != 1 {
		t.Fatalf("expected one cache flight, got %d", stats.CacheFlight.Load())
	}
	if stats.CacheWait.Load() == 0 {
		t.Fatalf("expected cache waiters to be observed")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d unexpected error: %v", i, err)
		}
		if results[i] != 42 {
			t.Fatalf("worker %d expected 42, got %d", i, results[i])
		}
	}
}

func TestCacheSingleflightPropagatesErrors(t *testing.T) {
	want := errors.New("boom")
	_, _, err := cacheSingleflight("singleflight-error", "q:test", func() (cacheValue, error) {
		return cacheValue{}, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}
