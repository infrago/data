package data

import (
	. "github.com/infrago/base"
)

// Configure 更新配置
func Configure(cfg Map) {
	module.Configure(cfg)
}

// Register 开放给外
func Register(name string, value Any) {
	module.Register(name, value)
}

// Ready 准备运行
// 此方法只单独使用模块时用
func Ready() {
	module.Initialize()
	module.Connect()
}

// Go 直接开跑
// 此方法只单独使用模块时用
func Go() {
	module.Initialize()
	module.Connect()
	module.Launch()
	module.Terminate()
}
