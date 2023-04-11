package data

import "errors"

const (
	NAME = "DATA"
)

const (
	CreateTrigger  = "$.data.create"
	ChangeTrigger  = "$.data.change"
	RemoveTrigger  = "$.data.remove"
	RecoverTrigger = "$.data.recover"
)

var (
	errInvalidDataConnection = errors.New("Invalid data connection.")
)

// const (
// 	DELIMS = `"` //字段以及表名边界符，自己实现数据驱动才需要处理这个，必须能启标识作用
// 	RANDBY = `$RANDBY$`

// 	COUNT = "COUNT"
// 	SUM   = "SUM"
// 	MAX   = "MAX"
// 	MIN   = "MIN"
// 	AVG   = "AVG"

// 	IS  = "="  //等于
// 	NOT = "!=" //不等于
// 	EQ  = "="  //等于
// 	NE  = "!=" //不等于
// 	NEQ = "!=" //不等于

// 	//约等于	正则等于
// 	AE   = "~*" //正则等于，约等于
// 	AEC  = "~"  //正则等于，区分大小写，
// 	RE   = "~*" //正则等于，约等于
// 	REC  = "~"  //正则等于，区分大小写，
// 	REQ  = "~*" //正则等于，约等于
// 	REQC = "~"  //正则等于，区分大小写，

// 	NAE   = "!~*" //正则不等于，
// 	NAEC  = "!~"  //正则不等于，区分大小写，
// 	NRE   = "!~*" //正则不等于，
// 	NREC  = "!~"  //正则不等于，区分大小写，
// 	NREQ  = "!~*" //正则不等于，
// 	NREQC = "!~"  //正则不等于，区分大小写，

// 	//换位约等于，值在前，字段在后，用于黑名单查询
// 	EA   = "$$~*$$" //正则等于，约等于
// 	EAC  = "$$~$$"  //正则等于，区分大小写，
// 	ER   = "$$~*$$" //正则等于，约等于
// 	ERC  = "$$~$$"  //正则等于，区分大小写，
// 	EQR  = "$$~*$$" //正则等于，约等于
// 	EQRC = "$$~$$"  //正则等于，区分大小写，

// 	NEA   = "$$!~*$$" //正则不等于，
// 	NEAC  = "$$!~$$"  //正则不等于，区分大小写，
// 	NER   = "$$!~*$$" //正则不等于，
// 	NERC  = "$$!~$$"  //正则不等于，区分大小写，
// 	NEQR  = "$$!~*$$" //正则不等于，
// 	NEQRC = "$$!~$$"  //正则不等于，区分大小写，

// 	GT  = ">"  //大于
// 	GE  = ">=" //大于等于
// 	GTE = ">=" //大于等于
// 	LT  = "<"  //小于
// 	LE  = "<=" //小于等于
// 	LTE = "<=" //小于等于

// 	IN    = "$$IN$$"    //支持  WHERE id IN (1,2,3)
// 	NI    = "$$NOTIN$$" //支持	WHERE id NOT IN(1,2,3)
// 	NIN   = "$$NOTIN$$" //支持	WHERE id NOT IN(1,2,3)
// 	ANY   = "$$ANY$$"   //支持数组字段的
// 	OR    = "$$OR$$"    //或操作，
// 	NOR   = "$$NOR$$"   //支持数组字段的
// 	CON   = "$$CON$$"   //包含 array contais @>
// 	CONBY = "$$CONBY$$" //包含 array is contais by <@

// 	SEARCH    = "$$full$$"  //like搜索
// 	FULLLIKE  = "$$full$$"  //like搜索
// 	LEFTLIKE  = "$$left$$"  //like left搜索
// 	RIGHTLIKE = "$$right$$" //like right搜索

// 	INC = "$$inc$$" //累加，    UPDATE时用，解析成：views=views+value

// 	BYASC  = "asc"
// 	BYDESC = "desc"
// )

// type (
// 	dataNil  struct{}
// 	dataNol  struct{}
// 	dataRand struct{}
// 	dataAsc  struct{}
// 	dataDesc struct{}
// )

// var (
// 	NIL  dataNil //为空	IS NULL
// 	NOL  dataNol //不为空	IS NOT NULL
// 	NULL dataNil //为空	IS NULL
// 	NOLL dataNol //不为空	IS NOT NULL
// 	RAND dataRand
// 	ASC  dataAsc  //正序	asc
// 	DESC dataDesc //倒序	desc
// )
