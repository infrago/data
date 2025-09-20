package data

import (
	"fmt"
	"strings"
	"sync"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

func init() {
	infra.Mount(module)
}

var (
	module = &Module{
		configs:   make(map[string]Config, 0),
		drivers:   make(map[string]Driver, 0),
		instances: make(map[string]*Instance, 0),
		tables:    make(map[string]Table, 0),
		views:     make(map[string]View, 0),
		models:    make(map[string]Model, 0),
	}
)

type (
	Module struct {
		mutex sync.Mutex

		// 几项运行状态
		connected, initialized, launched bool

		configs map[string]Config
		drivers map[string]Driver

		tables map[string]Table
		views  map[string]View
		models map[string]Model

		//连接
		instances map[string]*Instance
	}

	Configs map[string]Config
	Config  struct {
		Driver  string
		Url     string
		Schema  string
		Serial  string
		Setting Map
	}
	Instance struct {
		connect Connect
		Name    string
		Config  Config
		Setting Map
	}
)

// Driver 注册驱动
func (module *Module) Driver(name string, driver Driver) {
	module.mutex.Lock()
	defer module.mutex.Unlock()

	if driver == nil {
		panic("Invalid cache driver: " + name)
	}

	if infra.Override() {
		module.drivers[name] = driver
	} else {
		if module.drivers[name] == nil {
			module.drivers[name] = driver
		}
	}
}

func (this *Module) Config(name string, config Config) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}

	if infra.Override() {
		this.configs[name] = config
	} else {
		if _, ok := this.configs[name]; ok == false {
			this.configs[name] = config
		}
	}
}
func (this *Module) Configs(config Configs) {
	for key, val := range config {
		this.Config(key, val)
	}
}

// Instance
func (this *Module) Instance(names ...string) *Instance {
	if len(names) > 0 {
		if inst, ok := module.instances[names[0]]; ok {
			return inst
		}
	} else {
		for _, val := range this.instances {
			return val
		}
	}
	panic("Invalid data connection.")
}

// 返回数据Base对象
func (this *Module) Base(names ...string) DataBase {
	inst := module.Instance(names...)
	return inst.connect.Base()
}

//----------------------------------------------------------------------

// 查询语法解析器
// 字段包裹成  $field$ 请自行处理
// 如mysql为反引号`field`，postgres, oracle为引号"field"，
// 所有参数使用问号(?)表示
// postgres驱动需要自行处理转成 $1,$2这样的
// oracle驱动需要自行处理转成 :1 :2这样的
// mongodb不适用，需驱动自己实现
func (this *Module) Parse(args ...Any) (string, []Any, string, error) {

	if len(args) > 0 {

		//如果直接写sql
		if v, ok := args[0].(string); ok {
			sql := v
			params := []interface{}{}
			orderBy := ""

			for i, arg := range args {
				if i > 0 {
					params = append(params, arg)
				}
			}

			//这里要处理一下，把order提取出来
			//先拿到 order by 的位置
			i := strings.Index(strings.ToLower(sql), "order by")
			if i >= 0 {
				orderBy = sql[i:]
				sql = sql[:i]
			}

			return sql, params, orderBy, nil

		} else {

			maps := []Map{}
			for _, v := range args {
				if m, ok := v.(Map); ok {
					maps = append(maps, m)
				}
				//如果直接是[]Map，应该算OR处理啊，暂不处理这个
			}

			querys, values, orders := module.parsing(maps...)

			orderStr := ""
			if len(orders) > 0 {
				orderStr = fmt.Sprintf("ORDER BY %s", strings.Join(orders, ","))
			}

			//sql := fmt.Sprintf("%s %s", strings.Join(querys, " OR "), orderStr)

			if len(querys) == 0 {
				querys = append(querys, "1=1")
			}

			return strings.Join(querys, " OR "), values, orderStr, nil
		}
	} else {
		return "1=1", []Any{}, "", nil
	}
}

func (this *Module) orderby(key string) string {
	dots := strings.Split(key, ".")
	if len(dots) > 1 {
		return fmt.Sprintf(`COALESCE(("%s"->'%s')::float8, 0)`, dots[0], dots[1])
	}
	return key
}

// func (this *Module) fieldby(key string) string {
// 	dots := strings.Split(key, ".")
// 	if len(dots) > 1 {
// 		return fmt.Sprintf(`"%s"->'%s'`, dots[0], dots[1])
// 	}
// 	return key
// }

// 注意，这个是实际的解析，支持递归
func (this *Module) parsing(args ...Map) ([]string, []interface{}, []string) {

	querys := []string{}
	values := make([]interface{}, 0)
	orders := []string{}

	//否则是多个map,单个为 与, 多个为 或
	for _, m := range args {
		ands := []string{}

		for k, v := range m {

			// 字段名处理
			// 包含.应该是处理成json
			// 包含:就处理成数组
			jsoned := false
			if dots := strings.Split(k, ":"); len(dots) >= 2 {
				k = fmt.Sprintf(`%v%v%v[%v]`, DELIMS, dots[0], DELIMS, dots[1])
			} else if dots := strings.Split(k, "."); len(dots) >= 2 {
				//"%s"->'%s'
				jsoned = true
				k = fmt.Sprintf(`%v%v%v->>'%v'`, DELIMS, dots[0], DELIMS, dots[1])
			} else {
				k = fmt.Sprintf(`%v%v%v`, DELIMS, k, DELIMS)
			}

			//如果值是ASC,DESC，表示是排序
			//if ov,ok := v.(string); ok && (ov==ASC || ov==DESC) {
			if v == ASC {
				//正序
				orders = append(orders, fmt.Sprintf(`%s ASC`, module.orderby(k)))
			} else if v == DESC {
				//倒序
				orders = append(orders, fmt.Sprintf(`%s DESC`, module.orderby(k)))

			} else if v == RAND {
				//随机排序
				orders = append(orders, fmt.Sprintf(`%s ASC`, RANDBY))

			} else if v == nil {
				ands = append(ands, fmt.Sprintf(`%s IS NULL`, k))
			} else if v == NIL {
				ands = append(ands, fmt.Sprintf(`%s IS NULL`, k))
			} else if v == NOL {
				//不为空值
				ands = append(ands, fmt.Sprintf(`%s IS NOT NULL`, k))
				/*
				   }  else if _,ok := v.(Nil); ok {
				       //为空值
				       ands = append(ands, fmt.Sprintf(`%s IS NULL`, k))
				   } else if _,ok := v.(NotNil); ok {
				       //不为空值
				       ands = append(ands, fmt.Sprintf(`%s IS NOT NULL`, k))
				   } else if fts,ok := v.(FTS); ok {
				       //处理模糊搜索，此条后续版本会移除
				       safeFts := strings.Replace(string(fts), "'", "''", -1)
				       ands = append(ands, fmt.Sprintf(`%s LIKE '%%%s%%'`, k, safeFts))
				*/
			} else if ms, ok := v.([]Map); ok {
				//是[]Map，相当于or

				qs, vs, os := module.parsing(ms...)
				if len(qs) > 0 {
					ands = append(ands, fmt.Sprintf("(%s)", strings.Join(qs, " OR ")))
					for _, vsVal := range vs {
						values = append(values, vsVal)
					}
				}
				for _, osVal := range os {
					orders = append(orders, osVal)
				}

			} else if opMap, opOK := v.(Map); opOK {
				//v要处理一下如果是map要特别处理
				//key做为操作符，比如 > < >= 等
				//而且多个条件是and，比如 views > 1 AND views < 100
				//自定义操作符的时候，可以用  is not null 吗？
				//hai yao chu li INC in change update

				opAnds := []string{}
				for opKey, opVal := range opMap {
					//这里要支持LIKE
					if opKey == SEARCH {
						safeFts := strings.Replace(fmt.Sprintf("%v", opVal), "'", "''", -1)
						opAnds = append(opAnds, fmt.Sprintf(`upper(%s) LIKE upper('%%%s%%')`, k, safeFts))
					} else if opKey == FULLLIKE {
						safeFts := strings.Replace(fmt.Sprintf("%v", opVal), "'", "''", -1)
						opAnds = append(opAnds, fmt.Sprintf(`upper(%s) LIKE upper('%%%s%%')'`, k, safeFts))
					} else if opKey == LEFTLIKE {
						safeFts := strings.Replace(fmt.Sprintf("%v", opVal), "'", "''", -1)
						opAnds = append(opAnds, fmt.Sprintf(`upper(%s) LIKE upper('%s%%')`, k, safeFts))
					} else if opKey == RIGHTLIKE {
						safeFts := strings.Replace(fmt.Sprintf("%v", opVal), "'", "''", -1)
						opAnds = append(opAnds, fmt.Sprintf(`upper(%s) LIKE upper('%%%s')`, k, safeFts))
					} else if opKey == ANY {

						//ANY要支持 传过来数组的情况，这样相当于，2个数组对比，只要包含任意就匹配
						//相当于CON, CONBY的 或版本，CON,CONBY是与，要全部包含才匹配
						//OVERLAP交集，交叉只要有任何一个就可以匹配
						isArray := false
						switch opVal.(type) {
						case []string, []int, []int64, []float32, []float64, []bool:
							isArray = true
						}

						if !isArray {
							//为了照顾crdb，要强加类型转换
							switch vv := opVal.(type) {
							case string:
								opAnds = append(opAnds, fmt.Sprintf(`?::text = ANY(%s)`, k))
								values = append(values, vv)
							case int:
								opAnds = append(opAnds, fmt.Sprintf(`?::int8 = ANY(%s)`, k))
								values = append(values, int64(vv))
							case int64:
								opAnds = append(opAnds, fmt.Sprintf(`?::int8 = ANY(%s)`, k))
								values = append(values, vv)
							case float32:
								opAnds = append(opAnds, fmt.Sprintf(`?::float8 = ANY(%s)`, k))
								values = append(values, float64(vv))
							case float64:
								opAnds = append(opAnds, fmt.Sprintf(`?::float8 = ANY(%s)`, k))
								values = append(values, vv)
							case bool:
								opAnds = append(opAnds, fmt.Sprintf(`?::boolean = ANY(%s)`, k))
								values = append(values, vv)
							default:
								opAnds = append(opAnds, fmt.Sprintf(`?::text = ANY(%s)`, k))
								values = append(values, fmt.Sprintf("%v", vv))
							}

						} else {

							conArgs := []string{}
							conVals := []Any{}
							switch vs := opVal.(type) {
							case []string:
								for _, v := range vs {
									conArgs = append(conArgs, "?::text")
									conVals = append(conVals, v)
								}
							case []int:
								for _, v := range vs {
									conArgs = append(conArgs, "?::int8")
									conVals = append(conVals, int64(v))
								}
							case []int64:
								for _, v := range vs {
									conArgs = append(conArgs, "?::int8")
									conVals = append(conVals, int64(v))
								}
							case []float32:
								for _, v := range vs {
									conArgs = append(conArgs, "?::int8")
									conVals = append(conVals, float64(v))
								}
							case []float64:
								for _, v := range vs {
									conArgs = append(conArgs, "?::int8")
									conVals = append(conVals, float64(v))
								}
							case []bool:
								for _, v := range vs {
									conArgs = append(conArgs, "?::boolean")
									conVals = append(conVals, v)
								}
							case []Any:
								for _, v := range vs {
									conArgs = append(conArgs, "?::text")
									conVals = append(conVals, fmt.Sprintf("%v", v))
								}
							default:
								conArgs = append(conArgs, "?::text")
								conVals = append(conVals, fmt.Sprintf("%v", vs))
							}

							if len(conArgs) > 0 && len(conVals) > 0 {
								opAnds = append(opAnds, fmt.Sprintf(`%s && ARRAY[%s]`, k, strings.Join(conArgs, ",")))
								for _, v := range conVals {
									values = append(values, v)
								}
							} else {
								//为了当前节的and匹配不上数组，因为可能是空数组
								opAnds = append(opAnds, fmt.Sprintf(`1 = 2`))
							}

						}

					} else if opKey == OVERLAP {
						// array contains array @>

						conArgs := []string{}
						conVals := []Any{}
						switch vs := opVal.(type) {

						case int:
							conArgs = append(conArgs, "?::int8")
							conVals = append(conVals, vs)
						case []int:
							for _, v := range vs {
								conArgs = append(conArgs, "?::int8")
								conVals = append(conVals, v)
							}
						case int64:
							conArgs = append(conArgs, "?::int8")
							conVals = append(conVals, vs)
						case []int64:
							for _, v := range vs {
								conArgs = append(conArgs, "?::int8")
								conVals = append(conVals, v)
							}
						case float64:
							conArgs = append(conArgs, "?::float8")
							conVals = append(conVals, vs)
						case []float64:
							for _, v := range vs {
								conArgs = append(conArgs, "?::float8")
								conVals = append(conVals, v)
							}
						case string:
							conArgs = append(conArgs, "?::text")
							conVals = append(conVals, vs)
						case []string:
							for _, v := range vs {
								conArgs = append(conArgs, "?::text")
								conVals = append(conVals, v)
							}
						case []bool:
							for _, v := range vs {
								conArgs = append(conArgs, "?::boolean")
								conVals = append(conVals, v)
							}
						case []Any:
							for _, v := range vs {
								conArgs = append(conArgs, "?::text")
								conVals = append(conVals, fmt.Sprintf("%v", v))
							}
						default:
							conArgs = append(conArgs, "?::text")
							conVals = append(conVals, fmt.Sprintf("%v", vs))
						}

						if len(conArgs) > 0 && len(conVals) > 0 {
							opAnds = append(opAnds, fmt.Sprintf(`%s && ARRAY[%s]`, k, strings.Join(conArgs, ",")))
							for _, v := range conVals {
								values = append(values, v)
							}
						} else {
							//为了当前节的and匹配不上数组，因为可能是空数组
							opAnds = append(opAnds, fmt.Sprintf(`1 = 2`))
						}

					} else if opKey == CON {
						// array contains array @>

						conArgs := []string{}
						conVals := []Any{}
						switch vs := opVal.(type) {

						case int:
							conArgs = append(conArgs, "?::int8")
							conVals = append(conVals, vs)
						case []int:
							for _, v := range vs {
								conArgs = append(conArgs, "?::int8")
								conVals = append(conVals, v)
							}
						case int64:
							conArgs = append(conArgs, "?::int8")
							conVals = append(conVals, vs)
						case []int64:
							for _, v := range vs {
								conArgs = append(conArgs, "?::int8")
								conVals = append(conVals, v)
							}
						case float64:
							conArgs = append(conArgs, "?::float8")
							conVals = append(conVals, vs)
						case []float64:
							for _, v := range vs {
								conArgs = append(conArgs, "?::float8")
								conVals = append(conVals, v)
							}
						case string:
							conArgs = append(conArgs, "?::text")
							conVals = append(conVals, vs)
						case []string:
							for _, v := range vs {
								conArgs = append(conArgs, "?::text")
								conVals = append(conVals, v)
							}
						case []bool:
							for _, v := range vs {
								conArgs = append(conArgs, "?::boolean")
								conVals = append(conVals, v)
							}
						case []Any:
							for _, v := range vs {
								conArgs = append(conArgs, "?::text")
								conVals = append(conVals, fmt.Sprintf("%v", v))
							}
						default:
							conArgs = append(conArgs, "?::text")
							conVals = append(conVals, fmt.Sprintf("%v", vs))
						}

						if len(conArgs) > 0 && len(conVals) > 0 {
							opAnds = append(opAnds, fmt.Sprintf(`%s @> ARRAY[%s]`, k, strings.Join(conArgs, ",")))
							for _, v := range conVals {
								values = append(values, v)
							}
						} else {
							//为了当前节的and匹配不上数组，因为可能是空数组
							opAnds = append(opAnds, fmt.Sprintf(`1 = 2`))
						}

					} else if opKey == CONBY {
						// array contains by array <@

						conbyArgs := []string{}
						conbyVals := []Any{}
						switch vs := opVal.(type) {
						case int:
							conbyArgs = append(conbyArgs, "?::int8")
							conbyVals = append(conbyVals, vs)
						case []int:
							for _, v := range vs {
								conbyArgs = append(conbyArgs, "?::int8")
								conbyVals = append(conbyVals, v)
							}
						case int64:
							conbyArgs = append(conbyArgs, "?::int8")
							conbyVals = append(conbyVals, vs)
						case []int64:
							for _, v := range vs {
								conbyArgs = append(conbyArgs, "?::int8")
								conbyVals = append(conbyVals, v)
							}
						case float64:
							conbyArgs = append(conbyArgs, "?::float8")
							conbyVals = append(conbyVals, vs)
						case []float64:
							for _, v := range vs {
								conbyArgs = append(conbyArgs, "?::float8")
								conbyVals = append(conbyVals, v)
							}
						case string:
							conbyArgs = append(conbyArgs, "?::text")
							conbyVals = append(conbyVals, vs)
						case []string:
							for _, v := range vs {
								conbyArgs = append(conbyArgs, "?::text")
								conbyVals = append(conbyVals, v)
							}
						case bool:
							conbyArgs = append(conbyArgs, "?::boolean")
							conbyVals = append(conbyVals, vs)
						case []bool:
							for _, v := range vs {
								conbyArgs = append(conbyArgs, "?::boolean")
								conbyVals = append(conbyVals, v)
							}
						case []Any:
							for _, v := range vs {
								conbyArgs = append(conbyArgs, "?::text")
								conbyVals = append(conbyVals, fmt.Sprintf("%v", v))
							}
						default:
							conbyArgs = append(conbyArgs, "?::text")
							conbyVals = append(conbyVals, fmt.Sprintf("%v", vs))
						}

						if len(conbyArgs) > 0 && len(conbyVals) > 0 {
							opAnds = append(opAnds, fmt.Sprintf(`%s <@ ARRAY[%s]`, k, strings.Join(conbyArgs, ",")))
							for _, v := range conbyVals {
								values = append(values, v)
							}
							//为什么要包含空数组？20230110
							opAnds = append(opAnds, fmt.Sprintf(`%s <@ '{}'`, k))
						} else {
							//为了当前节的and匹配不上数组，因为可能是空数组
							opAnds = append(opAnds, fmt.Sprintf(`1 = 2`))
						}

					} else if opKey == OR {

						realArgs := []string{}
						realVals := []Any{}
						if vvs, ok := opVal.([]Any); ok {
							for _, vv := range vvs {
								if vv == nil {
									realArgs = append(realArgs, fmt.Sprintf(`%s is null`, k))
								} else {
									realArgs = append(realArgs, fmt.Sprintf(`%s=?`, k))
									realVals = append(realVals, vv)
								}

							}
						} else if vvs, ok := opVal.([]int64); ok {
							for _, vv := range vvs {
								realArgs = append(realArgs, fmt.Sprintf(`%s=?`, k))
								realVals = append(realVals, vv)
							}
						} else if vvs, ok := opVal.([]float64); ok {
							for _, vv := range vvs {
								realArgs = append(realArgs, fmt.Sprintf(`%s=?`, k))
								realVals = append(realVals, vv)
							}
						} else if vvs, ok := opVal.([]string); ok {
							for _, vv := range vvs {
								realArgs = append(realArgs, fmt.Sprintf(`%s=?`, k))
								realVals = append(realVals, vv)
							}
						}

						opAnds = append(opAnds, strings.Join(realArgs, " OR "))
						for _, v := range realVals {
							values = append(values, v)
						}

					} else if opKey == NOR {

						realArgs := []string{}
						realVals := []Any{}
						incNull := true
						if vvs, ok := opVal.([]Any); ok {
							for _, vv := range vvs {
								if vv == nil {
									incNull = false
								} else {
									realArgs = append(realArgs, fmt.Sprintf(`%s=?`, k))
									realVals = append(realVals, vv)
								}
							}
						} else if vvs, ok := opVal.([]int64); ok {
							for _, vv := range vvs {
								realArgs = append(realArgs, fmt.Sprintf(`%s=?`, k))
								realVals = append(realVals, vv)
							}
						} else if vvs, ok := opVal.([]float64); ok {
							for _, vv := range vvs {
								realArgs = append(realArgs, fmt.Sprintf(`%s==?`, k))
								realVals = append(realVals, vv)
							}
						} else if vvs, ok := opVal.([]string); ok {
							for _, vv := range vvs {
								realArgs = append(realArgs, fmt.Sprintf(`%s==?`, k))
								realVals = append(realVals, vv)
							}
						}

						if incNull {
							opAnds = append(opAnds, fmt.Sprintf(`NOT (%s) or %s is null`, strings.Join(realArgs, " OR "), k))
						} else {
							opAnds = append(opAnds, fmt.Sprintf(`NOT (%s)`, strings.Join(realArgs, " OR ")))
						}

						for _, v := range realVals {
							values = append(values, v)
						}

					} else if opKey == IN {
						//IN (?,?,?)

						realArgs := []string{}
						realVals := []Any{}
						switch vs := opVal.(type) {
						case []int:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						case []int64:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						case []string:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						case []Any:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						default:
							realArgs = append(realArgs, "?")
							realVals = append(realVals, vs)
						}

						opAnds = append(opAnds, fmt.Sprintf(`%s IN(%s)`, k, strings.Join(realArgs, ",")))
						for _, v := range realVals {
							values = append(values, v)
						}

					} else if opKey == NIN {
						//NOT IN (?,?,?)

						realArgs := []string{}
						realVals := []Any{}
						switch vs := opVal.(type) {
						case []int:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						case []int64:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						case []string:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						case []Any:
							if len(vs) > 0 {
								for _, v := range vs {
									realArgs = append(realArgs, "?")
									realVals = append(realVals, v)
								}
							} else {
								realArgs = append(realArgs, "?")
								realVals = append(realVals, 0)
							}
						default:
							realArgs = append(realArgs, "?")
							realVals = append(realVals, vs)
						}

						opAnds = append(opAnds, fmt.Sprintf(`%s NOT IN(%s)`, k, strings.Join(realArgs, ",")))
						for _, v := range realVals {
							values = append(values, v)
						}

					} else {
						opAnds = append(opAnds, fmt.Sprintf(`%s %s ?`, k, opKey))
						values = append(values, opVal)
					}
				}

				ands = append(ands, fmt.Sprintf("(%s)", strings.Join(opAnds, " AND ")))

			} else {
				ands = append(ands, fmt.Sprintf(`%s = ?`, k))
				if jsoned {
					values = append(values, fmt.Sprintf("%v", v))
				} else {
					values = append(values, v)
				}
			}
		}

		newAnds := []string{}
		for _, and := range ands {
			if and != "" && and != "()" {
				newAnds = append(newAnds, and)
			}
		}

		if len(newAnds) > 0 {
			querys = append(querys, fmt.Sprintf("(%s)", strings.Join(newAnds, " AND ")))
		}
	}

	return querys, values, orders
}
