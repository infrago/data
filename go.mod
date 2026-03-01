module github.com/infrago/data

go 1.25.3

require (
	github.com/infrago/base v0.8.2
	github.com/infrago/infra v0.8.2
)

require github.com/pelletier/go-toml/v2 v2.2.2 // indirect

replace github.com/infrago/infra => ../bamgoo

replace github.com/infrago/base => ../base
