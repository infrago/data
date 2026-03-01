# data

infrago data module with Mongo-like `Map` query DSL and SQL drivers.

## Naming Mapping

Disabled by default. Enable in config to map `camelCase <-> snake_case` automatically.

```toml
[data]
mapping = true
```

## Query DSL

- compare: `$eq $ne $gt $gte $lt $lte $in $nin`
- json/array: `$contains $overlap $elemMatch`
- logic: `$and $or $nor $not`
- text: `$like $ilike $regex`
- options: `$select $sort $limit $offset $after $group $having $join $agg $unsafe`

## Sort Notes

- Single-field sort: use `Map`
- Multi-field sort: use `[]Map` to preserve order

```go
// single-field
rows1 := db.Table("article").Query(base.Map{
  "$sort": base.Map{"id": base.DESC},
})
_ = rows1

// multi-field (ordered)
rows2 := db.Table("article").Query(base.Map{
  "$sort": []base.Map{
    {"id": base.ASC},
    {"views": base.DESC},
  },
})
_ = rows2
```

## Update DSL

- `$set`
- `$inc`
- `$unset`
- `$push`
- `$pull`
- `$addToSet`
- `$setPath`
- `$unsetPath`

## Driver Notes

- `pgsql`: native support for `$contains/$overlap/$elemMatch` (json/array operators).
- `mysql`: uses JSON functions for `$contains/$overlap/$elemMatch`.
- `sqlite`: `$contains` fallback is string-search; `$overlap/$elemMatch` are not supported directly.

## Capabilities

```go
caps, _ := data.GetCapabilities()
fmt.Println(caps)
// {Dialect:sqlite ILike:false Returning:false Join:true ...}
```

## Join Example

```go
rows, err := db.View("order").Query(base.Map{
  "$select": []string{"order.id", "order.amount", "user.name"},
  "$join": []base.Map{
    {
      "from": "user",
      "alias": "user",
      "type": "left",
      "localField": "order.user_id",
      "foreignField": "user.id",
    },
  },
  "user.status": "active",
  "$sort": base.Map{"order.id": base.ASC},
  "$limit": 20,
})
_ = rows
_ = err
```

## Keyset Pagination (`$after`)

```go
page1, _ := db.Table("user").Query(base.Map{
  "$sort": base.Map{"id": base.ASC},
  "$limit": 20,
})

page2, _ := db.Table("user").Query(base.Map{
  "$sort": base.Map{"id": base.ASC},
  "$after": base.Map{"id": page1[len(page1)-1]["id"]},
  "$limit": 20,
})
```

## Aggregate Example

```go
rows, _ := db.View("order").Aggregate(base.Map{
  "status": "paid",
  "$group": []string{"user_id"},
  "$agg": base.Map{
    "total_amount": base.Map{"sum": "amount"},
    "avg_amount":   base.Map{"avg": "amount"},
    "cnt":          base.Map{"count": "*"},
  },
  "$having": base.Map{
    "total_amount": base.Map{"$gt": 100},
  },
  "$sort": base.Map{"total_amount": base.DESC},
})
_ = rows
```

## Slice (total + items)

```go
total, items := db.Table("user").Slice(0, 20, base.Map{
  "status": "active",
  "$sort": base.Map{"id": base.DESC},
})
if db.Error() != nil {
  return
}
_ = total
_ = items
```

## Tx / Batch

```go
_ = db.Tx(func(tx data.DataBase) error {
  _, err := tx.Table("user").InsertMany([]base.Map{
    {"name": "A"},
    {"name": "B"},
  })
  return err
})
```

## Safety Guard

By default, full-table `Update/Delete` is blocked unless query has filter.

```go
_, err := db.Table("user").Delete(base.Map{
  "$unsafe": true,
})
_ = err
```

## Join Field Ref

```go
rows, _ := db.View("order").Query(base.Map{
  "$join": []base.Map{
    {"from": "user", "alias": "u", "on": base.Map{
      "order.user_id": base.Map{"$eq": "$field:u.id"},
    }},
  },
})
_ = rows
```

## JSON/Array Example

```go
users, _ := db.Table("user").Query(base.Map{
  "tags": base.Map{"$contains": []string{"go"}},
  "$sort": base.Map{"id": base.ASC},
})
_ = users
```

## Example

```go
db := data.Base()
defer db.Close()

item, _ := db.Table("user").Upsert(base.Map{
  "$set": base.Map{"name": "Alice"},
  "$inc": base.Map{"login_times": 1},
}, base.Map{"id": 1001})

_ = item

rows, _ := db.View("order").Aggregate(base.Map{
  "$group": []string{"user_id"},
  "$agg": base.Map{
    "total": base.Map{"sum": "amount"},
    "cnt":   base.Map{"count": "*"},
  },
  "$having": base.Map{"total": base.Map{"$gt": 100}},
})

_ = rows
```
