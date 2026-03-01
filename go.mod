module github.com/infrago/data

go 1.25.3

require (
	github.com/infrago/infra v0.0.0
	github.com/infrago/base v0.0.1
)

require github.com/pelletier/go-toml/v2 v2.2.2 // indirect

replace github.com/infrago/infra => ../infra

replace github.com/infrago/base => ../base
