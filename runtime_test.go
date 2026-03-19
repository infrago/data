package data

import "testing"

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
